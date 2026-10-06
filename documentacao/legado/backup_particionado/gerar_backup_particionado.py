# -*- coding: utf-8 -*-
r"""
Script de Backup Físico Particionado (Cold Backup) com Controle de Espaço em Disco
Origens: 
  - E:\PostgreSQL (191 GB)
  - E:\Programas  (0.9 GB)
Destino: J:\Meu Drive\PostgreSQL
Particionamento: ~1 GB por arquivo com proteção contra esgotamento de disco
"""

import os
import sys
import time
import shutil
import tarfile
import pathlib
from tqdm import tqdm

PASTAS_ORIGEM = [
    pathlib.Path(r"E:\PostgreSQL"),
    pathlib.Path(r"E:\Programas")
]
DESTINO_DIR = pathlib.Path(r"J:\Meu Drive\PostgreSQL")
TAMANHO_PARTE_BYTES = 1024 * 1024 * 1024  # 1 GB por parte
NIVEL_COMPRESSAO = 9  # Máxima compressão gzip
NOME_BASE = "PostgreSQL_completo_backup"

# Limite mínimo de segurança no disco C: (em GB)
# Se o cache do Google Drive consumir espaço e o C: cair abaixo deste limite, o script pausa
# e aguarda o Google Drive enviar os dados para a nuvem antes de continuar.
LIMITE_SEGURANCA_C_GB = 8.0

class MultiVolumeWriter:
    """Escreve o stream compactado dividindo em volumes de 1GB com verificação de disco"""
    def __init__(self, destino_dir: pathlib.Path, base_name: str, chunk_size: int):
        self.destino_dir = destino_dir
        self.base_name = base_name
        self.chunk_size = chunk_size
        self.part_num = 1
        self.bytes_in_current_part = 0
        self.total_bytes_written = 0
        self.current_file = None
        self._open_next_part()

    def _verificar_espaco_disco(self):
        """Pausa se o disco C: (onde fica o cache do Google Drive) estiver ficando cheio"""
        while True:
            try:
                uso_c = shutil.disk_usage("C:")
                livre_gb = uso_c.free / (1024**3)
                if livre_gb < LIMITE_SEGURANCA_C_GB:
                    tqdm.write(f"\n[ALERTA DE ESPAÇO] Disco C: com apenas {livre_gb:.1f} GB livres!")
                    tqdm.write("Aguardando o Google Drive fazer o upload para liberar cache local (checando a cada 15s)...")
                    time.sleep(15)
                else:
                    break
            except Exception:
                break

    def _open_next_part(self):
        if self.current_file:
            self.current_file.close()
            # Antes de abrir o próximo arquivo de 1GB, verifica se o disco C: tem espaço
            self._verificar_espaco_disco()

        part_name = f"{self.base_name}.part{self.part_num:03d}.tar.gz"
        self.current_path = self.destino_dir / part_name
        self.current_file = open(self.current_path, "wb")
        self.bytes_in_current_part = 0
        self.part_num += 1

    def write(self, data: bytes):
        if not data:
            return 0
        total_len = len(data)
        offset = 0
        while offset < total_len:
            space_left = self.chunk_size - self.bytes_in_current_part
            to_write = min(total_len - offset, space_left)
            self.current_file.write(data[offset:offset + to_write])
            self.bytes_in_current_part += to_write
            self.total_bytes_written += to_write
            offset += to_write
            if self.bytes_in_current_part >= self.chunk_size:
                self._open_next_part()
        return total_len

    def close(self):
        if self.current_file and not self.current_file.closed:
            self.current_file.close()

def garantir_estrutura_postgres(pastas):
    """Garante que diretórios estruturais obrigatórios do PostgreSQL existam antes do backup"""
    subdirs_obrigatorios = [
        "pg_commit_ts",
        "pg_dynshmem",
        "pg_logical/mappings",
        "pg_logical/snapshots",
        "pg_notify",
        "pg_replslot",
        "pg_serial",
        "pg_snapshots",
        "pg_stat_tmp",
        "pg_tblspc",
        "pg_twophase",
        "pg_wal/archive_status"
    ]
    for p in pastas:
        data_dir = p / "data"
        if data_dir.exists():
            for sub in subdirs_obrigatorios:
                (data_dir / sub).mkdir(parents=True, exist_ok=True)

def mapear_arquivos(pastas):
    print("Mapeando arquivos e diretórios de dados e programas...")
    garantir_estrutura_postgres(pastas)
    total_bytes = 0
    lista_itens = []
    for pasta in pastas:
        if not pasta.exists():
            print(f"AVISO: Pasta não encontrada: {pasta}")
            continue
        print(f"  -> Escaneando: {pasta}")
        # Inclui a própria pasta raiz
        lista_itens.append((pasta, pathlib.Path(pasta.name), 0, True))
        
        for root, dirs, files in os.walk(pasta):
            root_path = pathlib.Path(root)
            # Inclui todos os subdiretórios (garante pastas vazias como pg_notify no tar)
            for d in dirs:
                dir_p = root_path / d
                try:
                    arcname = dir_p.relative_to(pasta.parent)
                    lista_itens.append((dir_p, arcname, 0, True))
                except Exception as e:
                    print(f"Aviso ao ler diretório {dir_p}: {e}")

            # Inclui todos os arquivos
            for f in files:
                p = root_path / f
                try:
                    tamanho = p.stat().st_size
                    arcname = p.relative_to(pasta.parent)
                    lista_itens.append((p, arcname, tamanho, False))
                    total_bytes += tamanho
                except Exception as e:
                    print(f"Aviso ao ler arquivo {p}: {e}")
    return total_bytes, lista_itens

def executar_backup():
    DESTINO_DIR.mkdir(parents=True, exist_ok=True)

    uso_c_ini = shutil.disk_usage("C:").free / (1024**3)
    uso_e_ini = shutil.disk_usage("E:").free / (1024**3)

    print("=" * 75)
    print("INICIANDO BACKUP COMPLETO COM CONTROLE INTELIGENTE DE FLUXO")
    for p in PASTAS_ORIGEM:
        print(f"Origem:  {p}")
    print(f"Destino: {DESTINO_DIR}")
    print(f"Espaço livre atual no C: (Cache do Google Drive): {uso_c_ini:.1f} GB")
    print(f"Espaço livre atual no E: (Origem):               {uso_e_ini:.1f} GB")
    print(f"Tamanho de cada parte: {TAMANHO_PARTE_BYTES / (1024**3):.1f} GB")
    print(f"Nível de compressão: MÁXIMO (nível {NIVEL_COMPRESSAO})")
    print(f"Proteção ativa: pausa automática se C: atingir < {LIMITE_SEGURANCA_C_GB:.1f} GB livres")
    print("=" * 75)

    total_bytes, lista_itens = mapear_arquivos(PASTAS_ORIGEM)
    total_arquivos = sum(1 for item in lista_itens if not item[3])
    total_dirs = sum(1 for item in lista_itens if item[3])
    print(f"\nTotal mapeado: {total_arquivos:,} arquivos e {total_dirs:,} diretórios")
    print(f"Tamanho total original: {total_bytes / (1024**3):.2f} GB\n")

    if not lista_itens:
        print("Nenhum arquivo ou diretório encontrado para compactar.")
        return

    writer = MultiVolumeWriter(DESTINO_DIR, NOME_BASE, TAMANHO_PARTE_BYTES)

    inicio = time.time()
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Compactando tudo") as pbar:
        with tarfile.open(fileobj=writer, mode="w|gz", compresslevel=NIVEL_COMPRESSAO) as tar:
            for item_path, arcname, tamanho, is_dir in lista_itens:
                pbar.set_postfix_str(f"{item_path.name[:25]}", refresh=False)
                try:
                    tar.add(str(item_path), arcname=str(arcname), recursive=False)
                except Exception as e:
                    print(f"Erro ao arquivar {item_path}: {e}")
                if not is_dir:
                    pbar.update(tamanho)

    writer.close()
    duracao = time.time() - inicio

    partes_geradas = sorted(list(DESTINO_DIR.glob(f"{NOME_BASE}.part*.tar.gz")))
    tamanho_comprimido_total = sum(p.stat().st_size for p in partes_geradas)

    print("\n" + "=" * 75)
    print("BACKUP COMPLETO CONCLUÍDO COM SUCESSO!")
    print(f"Tempo total: {duracao / 60:.1f} minutos")
    print(f"Tamanho original:   {total_bytes / (1024**3):.2f} GB")
    print(f"Tamanho comprimido: {tamanho_comprimido_total / (1024**3):.2f} GB (Taxa: {tamanho_comprimido_total/total_bytes*100:.1f}%)")
    print(f"Total de partes de 1GB geradas: {len(partes_geradas)}")
    print(f"Arquivos salvos em: {DESTINO_DIR}")
    for p in partes_geradas:
        print(f"  -> {p.name} ({p.stat().st_size / (1024**2):.1f} MB)")
    print("=" * 75)

if __name__ == "__main__":
    executar_backup()
