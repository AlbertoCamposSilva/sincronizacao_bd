"""Formato do arquivo de lote (.lote): linhas JSON compactadas (zlib) e criptografadas em blocos (AES-256-GCM).

Estrutura do arquivo:
    "SBD1" | no (1 byte tamanho + utf-8) | seq (8 bytes) | blocos...
    bloco = tipo(1: 0=dados, 1=rodape) | indice(8) | nonce(12) | tamanho(4) | texto cifrado
A autenticacao (AAD) amarra cada bloco ao no, ao sequencial, ao tipo e ao indice: embaralhar, remover ou trocar blocos
entre arquivos e detectado. O rodape (ultimo bloco) traz as contagens; arquivo sem rodape valido esta TRUNCADO.
A gravacao e atomica: escreve em .tmp, sincroniza em disco e so entao renomeia.
"""
import base64
import json
import os
import pathlib
import struct
import zlib
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIA = b"SBD1"
VERSAO_PROTOCOLO = 1
TAM_BLOCO = 4 * 1024 * 1024  # bytes de JSON por bloco, antes de compactar


class LoteInvalido(Exception):
    """Arquivo truncado, corrompido, de outro no/sequencial ou com chave errada."""


def chave_de_texto(b64: str) -> bytes:
    chave = base64.b64decode(b64.strip())
    if len(chave) != 32:
        raise ValueError("A chave dos lotes (SYNC_BD_CHAVE) precisa ter 32 bytes em base64.")
    return chave


def gerar_chave() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def nome_arquivo(no: str, seq: int) -> str:
    return f"{no}_{seq:09d}.lote"


def _aad(no: str, seq: int, tipo: int, idx: int) -> bytes:
    return MAGIA + no.encode() + struct.pack(">QBQ", seq, tipo, idx)


class EscritorLote:
    """Uso: e = EscritorLote(pasta, no, seq, chave); e.escrever(dict) ...; e.fechar(rodape) -> caminho final."""

    def __init__(self, pasta, no: str, seq: int, chave: bytes):
        self.pasta, self.no, self.seq = pathlib.Path(pasta), no, seq
        self.aes = AESGCM(chave)
        self.final = self.pasta / nome_arquivo(no, seq)
        self.tmp = self.pasta / (self.final.name + ".tmp")
        self.pasta.mkdir(parents=True, exist_ok=True)
        self.f = open(self.tmp, "wb")
        nb = no.encode()
        self.f.write(MAGIA + struct.pack(">B", len(nb)) + nb + struct.pack(">Q", seq))
        self.buf, self.buf_tam, self.idx, self.linhas = [], 0, 0, 0

    def escrever(self, registro: dict):
        linha = json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n"
        self.buf.append(linha)
        self.buf_tam += len(linha)
        self.linhas += 1
        if self.buf_tam >= TAM_BLOCO:
            self._descarregar()

    def _bloco(self, tipo: int, dados: bytes):
        nonce = os.urandom(12)
        cifrado = self.aes.encrypt(nonce, zlib.compress(dados, 6), _aad(self.no, self.seq, tipo, self.idx))
        self.f.write(struct.pack(">BQ", tipo, self.idx) + nonce + struct.pack(">I", len(cifrado)) + cifrado)
        self.idx += 1

    def _descarregar(self):
        if self.buf:
            self._bloco(0, "".join(self.buf).encode("utf-8"))
            self.buf, self.buf_tam = [], 0

    def fechar(self, rodape: dict) -> pathlib.Path:
        self._descarregar()
        self._bloco(1, json.dumps({**rodape, "linhas": self.linhas}, ensure_ascii=False).encode("utf-8"))
        self.f.flush()
        os.fsync(self.f.fileno())
        self.f.close()
        os.replace(self.tmp, self.final)
        return self.final

    def abortar(self):
        try:
            self.f.close()
        finally:
            if self.tmp.exists():
                self.tmp.unlink()


def ler_lote(caminho, chave: bytes):
    """Gera cada registro (dict) e, por ultimo, {'__rodape__': {...}, '__no__': no, '__seq__': seq}.
    Para ter certeza de que o arquivo e integro ANTES de aplicar, use verificar_lote()."""
    aes = AESGCM(chave)
    with open(caminho, "rb") as f:
        if f.read(4) != MAGIA:
            raise LoteInvalido("assinatura invalida (nao e um lote)")
        try:
            tn = f.read(1)[0]
            no = f.read(tn).decode()
            seq = struct.unpack(">Q", f.read(8))[0]
        except Exception as e:
            raise LoteInvalido("cabecalho truncado") from e
        esperado = 0
        viu_rodape = False
        while True:
            cab = f.read(9)
            if not cab:
                break
            if viu_rodape:
                raise LoteInvalido("dados depois do rodape")
            if len(cab) < 9:
                raise LoteInvalido("bloco truncado")
            tipo, idx = struct.unpack(">BQ", cab)
            nonce = f.read(12)
            tam_b = f.read(4)
            if len(nonce) < 12 or len(tam_b) < 4:
                raise LoteInvalido("bloco truncado")
            tam = struct.unpack(">I", tam_b)[0]
            cifrado = f.read(tam)
            if len(cifrado) < tam:
                raise LoteInvalido("bloco truncado")
            if idx != esperado:
                raise LoteInvalido(f"bloco fora de ordem ({idx} em vez de {esperado})")
            esperado += 1
            try:
                dados = zlib.decompress(aes.decrypt(nonce, cifrado, _aad(no, seq, tipo, idx)))
            except InvalidTag as e:
                raise LoteInvalido("falha de autenticacao (arquivo corrompido ou chave errada)") from e
            if tipo == 1:
                viu_rodape = True
                yield {"__rodape__": json.loads(dados), "__no__": no, "__seq__": seq}
            else:
                for linha in dados.decode("utf-8").splitlines():
                    yield json.loads(linha)
        if not viu_rodape:
            raise LoteInvalido("arquivo truncado (sem rodape)")


def verificar_lote(caminho, chave: bytes) -> dict:
    """Le o arquivo inteiro conferindo a autenticacao. Devolve o item de rodape. Lanca LoteInvalido se algo falhar."""
    rodape = None
    linhas = 0
    for item in ler_lote(caminho, chave):
        if "__rodape__" in item:
            rodape = item
        else:
            linhas += 1
    if rodape["__rodape__"]["linhas"] != linhas:
        raise LoteInvalido("contagem de linhas diverge do rodape")
    return rodape
