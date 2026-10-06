"""Configuração do sincronizador: nó (casa/cnpq), banco, pasta do Drive, chave dos lotes e políticas por tabela."""
import dataclasses
import os
import pathlib
import tomllib

NOS = ("casa", "cnpq")
MARCADOR = ".sincronizacao_bd.json"
PASTA_PADRAO = r"G:\Meu Drive\SincronizacaoBD"
RAIZ_PROJETO = pathlib.Path(__file__).resolve().parent.parent


def outro_no(no: str) -> str:
    if no not in NOS:
        raise ValueError(f"nó inválido: {no!r} (use um de {NOS})")
    return NOS[1] if no == NOS[0] else NOS[0]


@dataclasses.dataclass
class Config:
    no: str
    dsn: dict
    pasta: pathlib.Path
    chave: bytes
    slot: str = "sinc_slot"
    publicacao: str = "sinc_pub"
    prefixo_origem: str = "de_"          # origens de replicação: de_casa / de_cnpq (cluster inteiro; testes usam outro prefixo)
    tam_max_lote: int = 64 * 1024 * 1024
    ocioso_s: float = 3.0               # sem mensagens por tanto tempo = acabou o que havia no slot
    retencao_dias: int = 7
    espera_lote_faltando_h: float = 6.0
    # recepção da nuvem (Cloud SQL -> local): só o PC "puxador" liga puxar_nuvem
    puxar_nuvem: bool = False
    nuvem_intervalo_min: float = 30.0
    nuvem_falha_alerta_h: float = 6.0
    nuvem_espera_max_s: float = 1200.0
    gcp_projeto: str = "var-cnpq-2026"
    gcp_regiao: str = "southamerica-east1"
    nuvem_job: str = "var-sync-exportar"
    nuvem_bucket: str = "var-cnpq-2026-db-sync"
    nuvem_prefixo: str = "nuvem_para_local"
    # tabelas só de inserção (logs/custos): id repetido com conteúdo diferente vira linha nova, não sobrescreve
    somente_insercao: tuple = ("llm_registros_custos", "rag_auditoria_acesso", "log_importacoes_diarias", "pre_selecao_auditoria")
    pasta_logs: pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path(os.environ.get("LOCALAPPDATA", str(pathlib.Path.home()))) / "SincronizacaoBD" / "logs")

    def __post_init__(self):
        outro_no(self.no)
        self.pasta = pathlib.Path(self.pasta)

    @property
    def par(self) -> str:
        return outro_no(self.no)

    @property
    def origem_do_par(self) -> str:
        return f"{self.prefixo_origem}{self.par}"

    @property
    def origens(self) -> tuple:
        return tuple(f"{self.prefixo_origem}{n}" for n in NOS)

    @property
    def saida(self) -> pathlib.Path:
        return self.pasta / self.no / "saida"

    @property
    def saida_do_par(self) -> pathlib.Path:
        return self.pasta / self.par / "saida"

    @property
    def estado_proprio(self) -> pathlib.Path:
        return self.pasta / self.no / "estado.json"

    @property
    def estado_do_par(self) -> pathlib.Path:
        return self.pasta / self.par / "estado.json"


def localizar_pasta(preferida: str | None = None) -> pathlib.Path:
    """Pasta do Drive: a configurada; senão G:\\Meu Drive\\SincronizacaoBD; senão qualquer unidade com o marcador."""
    candidatas = [pathlib.Path(preferida)] if preferida else []
    candidatas.append(pathlib.Path(PASTA_PADRAO))
    for letra in "GHIJKLMNOPQRSTUVWXYZDEF":
        candidatas.append(pathlib.Path(f"{letra}:\\Meu Drive\\SincronizacaoBD"))
    for c in candidatas:
        try:
            if (c / MARCADOR).exists():
                return c
        except OSError:
            continue
    return pathlib.Path(preferida or PASTA_PADRAO)


def _segredo(nome: str, padrao=None):
    if nome in os.environ:
        return os.environ[nome]
    from acs_toolbox.segredos import get_secret
    return get_secret(nome, default=padrao)


def carregar(caminho: str | None = None, exigir_chave: bool = True) -> Config:
    """Lê config.toml (e config.local.toml) da raiz do projeto; credenciais e chave vêm do cofre acs-toolbox."""
    dados = {}
    for nome in ("config.toml", "config.local.toml"):
        p = pathlib.Path(caminho) if (caminho and nome == "config.toml") else RAIZ_PROJETO / nome
        if p.exists():
            with open(p, "rb") as f:
                dados.update(tomllib.load(f))
    no = os.environ.get("SINC_NO") or dados.get("no")
    if not no:
        raise RuntimeError('Defina o nó (casa ou cnpq) em config.local.toml (no = "casa") ou na variável SINC_NO.')
    dsn = dict(
        host=dados.get("db_host") or _segredo("POSTGRES_HOST") or _segredo("DB_HOST", "localhost"),
        port=int(dados.get("db_port") or _segredo("POSTGRES_PORT") or _segredo("DB_PORT", "5432")),
        dbname=dados.get("db_nome") or _segredo("POSTGRES_DATABASE") or _segredo("DB_NAME", "cnpq"),
        user=dados.get("db_usuario") or _segredo("POSTGRES_USER") or _segredo("DB_USER", "postgres"),
        password=_segredo("POSTGRES_PASSWORD") or _segredo("DB_PASSWORD") or _segredo("DB_PASS"),
    )
    from .lote import chave_de_texto
    chave_txt = _segredo("SYNC_BD_CHAVE")
    if not chave_txt and exigir_chave:
        raise RuntimeError("Falta o segredo SYNC_BD_CHAVE (gere com: python -m sincronizador gerar-chave).")
    cfg = Config(no=no, dsn=dsn, pasta=localizar_pasta(dados.get("pasta_drive")),
                 chave=chave_de_texto(chave_txt) if chave_txt else b"\x00" * 32)
    for campo in ("slot", "publicacao", "prefixo_origem", "tam_max_lote", "ocioso_s", "retencao_dias", "espera_lote_faltando_h",
                  "puxar_nuvem", "nuvem_intervalo_min", "nuvem_falha_alerta_h", "nuvem_espera_max_s", "gcp_projeto",
                  "gcp_regiao", "nuvem_job", "nuvem_bucket", "nuvem_prefixo"):
        if campo in dados:
            setattr(cfg, campo, dados[campo])
    if "somente_insercao" in dados:
        cfg.somente_insercao = tuple(dados["somente_insercao"])
    return cfg
