"""Decodificador do protocolo lógico `pgoutput` (proto_version 1), mensagens que o PostgreSQL envia ao slot.

Referência: "Logical Replication Message Formats" na documentação do PostgreSQL. Tudo em big-endian.
Cada mensagem vira um dict simples (serializável em JSON), pronto para ir ao lote.
"""
import struct
from datetime import datetime, timedelta, timezone

EPOCA_PG = datetime(2000, 1, 1, tzinfo=timezone.utc)


def lsn_para_texto(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def texto_para_lsn(txt: str) -> int:
    alto, baixo = txt.split("/")
    return (int(alto, 16) << 32) | int(baixo, 16)


def ts_pg_para_iso(micros: int) -> str:
    return (EPOCA_PG + timedelta(microseconds=micros)).isoformat()


class Leitor:
    def __init__(self, dados: bytes, pos: int = 0):
        self.d, self.p = dados, pos

    def i8(self):
        v = self.d[self.p]; self.p += 1; return v

    def i16(self):
        v = struct.unpack_from(">h", self.d, self.p)[0]; self.p += 2; return v

    def i32(self):
        v = struct.unpack_from(">i", self.d, self.p)[0]; self.p += 4; return v

    def u32(self):
        v = struct.unpack_from(">I", self.d, self.p)[0]; self.p += 4; return v

    def i64(self):
        v = struct.unpack_from(">q", self.d, self.p)[0]; self.p += 8; return v

    def u64(self):
        v = struct.unpack_from(">Q", self.d, self.p)[0]; self.p += 8; return v

    def texto(self):
        fim = self.d.index(b"\x00", self.p)
        s = self.d[self.p:fim].decode("utf-8"); self.p = fim + 1; return s

    def bytes_(self, n):
        v = self.d[self.p:self.p + n]; self.p += n; return v

    def char(self):
        return chr(self.i8())


def _tupla(r: Leitor) -> list:
    """Lista de valores por coluna: str (texto), None (NULL) ou {"u": 1} (TOAST não alterado, valor ausente)."""
    n = r.i16()
    valores = []
    for _ in range(n):
        tipo = r.char()
        if tipo == "n":
            valores.append(None)
        elif tipo == "u":
            valores.append({"u": 1})
        elif tipo in ("t", "b"):
            tam = r.i32()
            bruto = r.bytes_(tam)
            valores.append(bruto.decode("utf-8") if tipo == "t" else {"b": bruto.hex()})
        else:
            raise ValueError(f"tipo de coluna desconhecido na tupla: {tipo!r}")
    return valores


def decodificar(payload: bytes) -> dict:
    r = Leitor(payload)
    tipo = r.char()
    if tipo == "B":
        return {"t": "B", "lsn_final": r.u64(), "ts": ts_pg_para_iso(r.i64()), "xid": r.u32()}
    if tipo == "C":
        r.i8()
        return {"t": "C", "lsn_commit": r.u64(), "lsn_fim": r.u64(), "ts": ts_pg_para_iso(r.i64())}
    if tipo == "O":
        return {"t": "O", "lsn": r.u64(), "nome": r.texto()}
    if tipo == "R":
        relid = r.u32(); esquema = r.texto(); nome = r.texto(); ident = r.char(); n = r.i16()
        cols = []
        for _ in range(n):
            flags = r.i8(); nm = r.texto(); oid = r.u32(); mod = r.i32()
            cols.append({"nome": nm, "chave": bool(flags & 1), "oid": oid, "mod": mod})
        return {"t": "R", "relid": relid, "esquema": esquema, "tabela": nome, "ident": ident, "cols": cols}
    if tipo == "Y":
        return {"t": "Y", "oid": r.u32(), "esquema": r.texto(), "nome": r.texto()}
    if tipo == "I":
        relid = r.u32(); r.char()
        return {"t": "I", "relid": relid, "novo": _tupla(r)}
    if tipo == "U":
        relid = r.u32(); velho = None; chave_velha = None
        marca = r.char()
        if marca in ("K", "O"):
            t = _tupla(r)
            velho, chave_velha = (t, marca)
            marca = r.char()
        return {"t": "U", "relid": relid, "velho": velho, "tipo_velho": chave_velha, "novo": _tupla(r)}
    if tipo == "D":
        relid = r.u32(); marca = r.char()
        return {"t": "D", "relid": relid, "tipo_velho": marca, "velho": _tupla(r)}
    if tipo == "T":
        n = r.i32(); opcoes = r.i8()
        return {"t": "T", "relids": [r.u32() for _ in range(n)], "cascade": bool(opcoes & 1), "restart": bool(opcoes & 2)}
    if tipo == "M":  # mensagem lógica (não usamos)
        return {"t": "M"}
    raise ValueError(f"mensagem pgoutput desconhecida: {tipo!r}")
