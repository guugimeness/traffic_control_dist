\
"""
Persistência local de checkpoints com escrita atômica.

O módulo foi projetado para sobreviver a interrupções abruptas, como:

    docker kill <container>

Estratégia adotada:
1. O estado é serializado em JSON.
2. É calculado um SHA-256 sobre o conteúdo do estado.
3. A gravação ocorre primeiro em um arquivo temporário.
4. O arquivo é sincronizado em disco com fsync().
5. O checkpoint anterior é preservado como backup.
6. O arquivo temporário substitui atomicamente o checkpoint principal.

Caso o checkpoint principal esteja corrompido, o carregamento tenta
automaticamente usar a cópia de segurança.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Mapping


class CheckpointError(Exception):
    """Erro genérico relacionado a checkpoints."""


class CheckpointCorruptedError(CheckpointError):
    """Indica que um checkpoint existe, mas está inválido ou corrompido."""


class AtomicCheckpointStore:
    """
    Gerencia um único arquivo de checkpoint.

    Parameters
    ----------
    path:
        Caminho do arquivo JSON que armazenará o checkpoint.

    keep_backup:
        Quando True, mantém uma cópia do checkpoint anterior com extensão
        ".bak". Essa cópia é usada automaticamente se o arquivo principal
        estiver corrompido.
    """

    FORMAT_VERSION = 1

    def __init__(self, path: str | os.PathLike[str], keep_backup: bool = True):
        self.path = Path(path)
        self.backup_path = Path(f"{self.path}.bak")
        self.temp_path = Path(f"{self.path}.tmp")
        self.keep_backup = keep_backup
        self._lock = threading.RLock()

    @staticmethod
    def _canonical_json(data: Any) -> str:
        """
        Converte o objeto em JSON determinístico.

        sort_keys=True garante que o checksum seja reproduzível.
        separators reduz espaços desnecessários no conteúdo usado no hash.
        """
        return json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _checksum(cls, state: Any) -> str:
        serialized = cls._canonical_json(state)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        """
        Tenta sincronizar os metadados do diretório no Linux.

        Em algumas plataformas, abrir diretórios dessa forma não é suportado.
        Nesses casos, a falha é ignorada porque o fsync do arquivo já foi feito.
        """
        try:
            descriptor = os.open(str(directory), os.O_RDONLY)
        except OSError:
            return

        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _build_document(self, state: Mapping[str, Any]) -> dict[str, Any]:
        state_copy = dict(state)

        return {
            "format_version": self.FORMAT_VERSION,
            "saved_at": time.time(),
            "checksum": self._checksum(state_copy),
            "state": state_copy,
        }

    def save(self, state: Mapping[str, Any]) -> None:
        """
        Salva um checkpoint de maneira atômica.

        Raises
        ------
        TypeError:
            Se state não for um mapeamento/dicionário ou não puder ser
            serializado em JSON.

        CheckpointError:
            Se ocorrer uma falha de entrada/saída durante a persistência.
        """
        if not isinstance(state, Mapping):
            raise TypeError("O estado do checkpoint deve ser um dicionário.")

        document = self._build_document(state)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            try:
                with self.temp_path.open("w", encoding="utf-8") as file:
                    json.dump(
                        document,
                        file,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                    file.write("\n")
                    file.flush()
                    os.fsync(file.fileno())

                if self.keep_backup and self.path.exists():
                    shutil.copy2(self.path, self.backup_path)

                    with self.backup_path.open("rb") as backup_file:
                        os.fsync(backup_file.fileno())

                os.replace(self.temp_path, self.path)
                self._fsync_directory(self.path.parent)

            except (OSError, TypeError, ValueError) as error:
                try:
                    self.temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

                raise CheckpointError(
                    f"Não foi possível salvar o checkpoint em "
                    f"'{self.path}': {error}"
                ) from error

    def _read_and_validate(self, path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as file:
                document = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            raise CheckpointCorruptedError(
                f"O checkpoint '{path}' não pôde ser lido: {error}"
            ) from error

        if not isinstance(document, dict):
            raise CheckpointCorruptedError(
                f"O checkpoint '{path}' não contém um objeto JSON válido."
            )

        version = document.get("format_version")
        if version != self.FORMAT_VERSION:
            raise CheckpointCorruptedError(
                f"Versão de checkpoint incompatível em '{path}': "
                f"esperado={self.FORMAT_VERSION}, encontrado={version}."
            )

        state = document.get("state")
        checksum = document.get("checksum")

        if not isinstance(state, dict):
            raise CheckpointCorruptedError(
                f"O checkpoint '{path}' não possui um estado válido."
            )

        expected_checksum = self._checksum(state)

        if checksum != expected_checksum:
            raise CheckpointCorruptedError(
                f"Checksum inválido no checkpoint '{path}'."
            )

        return state

    def load(self, default: Any = None) -> dict[str, Any] | Any:
        """
        Carrega o checkpoint mais recente.

        Se o arquivo principal estiver corrompido, tenta a cópia ".bak".
        Se nenhum checkpoint existir, retorna default.

        Parameters
        ----------
        default:
            Valor retornado quando não existe checkpoint.

        Returns
        -------
        dict ou default
            Estado restaurado.
        """
        with self._lock:
            if self.path.exists():
                try:
                    return self._read_and_validate(self.path)
                except CheckpointCorruptedError as main_error:
                    if self.keep_backup and self.backup_path.exists():
                        try:
                            recovered_state = self._read_and_validate(
                                self.backup_path
                            )
                            print(
                                "[CHECKPOINT] Arquivo principal inválido. "
                                "Estado recuperado pela cópia de segurança."
                            )
                            return recovered_state
                        except CheckpointCorruptedError as backup_error:
                            raise CheckpointCorruptedError(
                                f"Checkpoint principal e backup estão "
                                f"corrompidos. Principal: {main_error} "
                                f"Backup: {backup_error}"
                            ) from backup_error

                    raise

            if self.keep_backup and self.backup_path.exists():
                return self._read_and_validate(self.backup_path)

            return default

    def exists(self) -> bool:
        """Retorna True se o checkpoint principal ou o backup existir."""
        return self.path.exists() or (
            self.keep_backup and self.backup_path.exists()
        )

    def delete(self) -> None:
        """Remove checkpoint principal, temporário e backup."""
        with self._lock:
            for file_path in (
                self.path,
                self.temp_path,
                self.backup_path,
            ):
                try:
                    file_path.unlink(missing_ok=True)
                except OSError as error:
                    raise CheckpointError(
                        f"Não foi possível remover '{file_path}': {error}"
                    ) from error


def save_checkpoint(
    path: str | os.PathLike[str],
    state: Mapping[str, Any],
    keep_backup: bool = True,
) -> None:
    """
    Função de conveniência para salvar um checkpoint.

    Exemplo
    -------
    save_checkpoint(
        "/data/subscriber_1.json",
        {"leader_id": 4, "local_vc": {"A": 10}}
    )
    """
    store = AtomicCheckpointStore(path, keep_backup=keep_backup)
    store.save(state)


def load_checkpoint(
    path: str | os.PathLike[str],
    default: Any = None,
    keep_backup: bool = True,
) -> dict[str, Any] | Any:
    """
    Função de conveniência para carregar um checkpoint.

    Exemplo
    -------
    state = load_checkpoint("/data/subscriber_1.json", default={})
    """
    store = AtomicCheckpointStore(path, keep_backup=keep_backup)
    return store.load(default=default)
