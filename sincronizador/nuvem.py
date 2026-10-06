"""Recepção da nuvem (Cloud SQL -> local), SOMENTE nesse sentido. Só o PC "puxador" (config: puxar_nuvem = true) roda.

Fluxo (a cada nuvem_intervalo_min, dentro do ciclo):
  1. dispara o Cloud Run Job `var-sync-exportar` (ele lê o Cloud SQL com um papel só-SELECT e grava CSV.gz + manifesto.json
     em gs://<bucket>/<prefixo>/<execução>/);
  2. baixa, confere o sha256 de cada arquivo contra o manifesto;
  3. mescla no banco local, numa única transação, SEM origem de replicação: o publicador deste PC captura a escrita e ela
     chega ao outro PC pelo fluxo normal (caminho único: nuvem -> puxador -> par);
  4. apaga a pasta da execução no bucket.

Regras (ver documentacao/NUVEM_CONTRATO.md):
  * tabelas "retrato": upsert por id (só escreve se mudou) + exclusão SÓ do que veio da nuvem (sincronizacao.nuvem_ids);
  * tabelas "incremental" (só inserção): insere com id local novo e guarda id_nuvem -> id_local; avança a marca;
  * nunca apaga linha criada localmente; freio de segurança se o retrato apagaria muita coisa de uma vez;
  * regra 9 do VAR: alerta (sem corrigir) se alberto.silva@cnpq.br não tiver exatamente ['administrador','CNPq'].
"""
import datetime
import gzip
import hashlib
import json
import logging
import pathlib
import secrets
import shutil
import time
import urllib.parse

import psycopg2

from . import avisos

log = logging.getLogger("sincronizador.nuvem")

VERSAO_CONTRATO = 1
# Ordem das FKs (pais antes dos filhos). Tabelas fora desta lista, mesmo que venham no manifesto, são ignoradas.
ORDEM = ["rag_usuarios", "rag_sessoes_chat", "rag_mensagens_chat", "rag_sessoes_documentos", "rag_gems_usuarios",
         "rag_gem_documentos_usuarios", "rag_feedbacks", "tarefas_apresentacao", "rag_deep_research_tarefas",
         "llm_registros_custos", "rag_auditoria_acesso"]
# D4: o código de acesso de uso único nunca entra no banco local
COLUNAS_PROIBIDAS = {"rag_usuarios": {"senha_temporaria", "expiracao_senha"}}
ADMIN_EMAIL = "alberto.silva@cnpq.br"
ADMIN_CARGOS = ["CNPq", "administrador"]
FREIO_MIN_LINHAS = 20          # exclusão em massa: só trava se passar de N linhas E de 50% do que veio da nuvem
FREIO_FRACAO = 0.5
NULO = r"\N"


class NuvemErro(Exception):
    pass


# --------------------------------------------------------------------------------------------------------------
# Transportes: o real (REST do Cloud Run + Cloud Storage com ADC) e o de teste (pasta local)
# --------------------------------------------------------------------------------------------------------------
class TransporteGCP:
    """Cloud Run Job + Cloud Storage por REST, com as credenciais ADC do usuário (as mesmas do acs-toolbox). Sem gcloud."""

    def __init__(self, cfg):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        cred, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.s = AuthorizedSession(cred)
        self.cfg = cfg

    def disparar(self, prefixo: str, marcas: dict) -> None:
        args = ["--prefixo", prefixo] + [a for t, m in sorted(marcas.items()) for a in ("--marca", f"{t}={m}")]
        c = self.cfg
        url = f"https://run.googleapis.com/v2/projects/{c.gcp_projeto}/locations/{c.gcp_regiao}/jobs/{c.nuvem_job}:run"
        r = self.s.post(url, json={"overrides": {"containerOverrides": [{"args": args}]}}, timeout=60)
        r.raise_for_status()
        op = r.json()
        limite = time.time() + c.nuvem_espera_max_s
        while not op.get("done"):
            if time.time() > limite:
                raise NuvemErro(f"o Job {c.nuvem_job} não terminou em {c.nuvem_espera_max_s:.0f} s")
            time.sleep(5)
            g = self.s.get(f"https://run.googleapis.com/v2/{op['name']}", timeout=60)
            g.raise_for_status()
            op = g.json()
        if op.get("error"):
            raise NuvemErro(f"o Job {c.nuvem_job} falhou: {op['error'].get('message', op['error'])}")

    def listar(self, prefixo: str) -> list:
        nomes, token = [], None
        while True:
            params = {"prefix": prefixo, "fields": "items(name),nextPageToken"}
            if token:
                params["pageToken"] = token
            r = self.s.get(f"https://storage.googleapis.com/storage/v1/b/{self.cfg.nuvem_bucket}/o", params=params, timeout=60)
            r.raise_for_status()
            d = r.json()
            nomes += [i["name"] for i in d.get("items", [])]
            token = d.get("nextPageToken")
            if not token:
                return nomes

    def baixar(self, nome: str, destino: pathlib.Path) -> None:
        url = (f"https://storage.googleapis.com/storage/v1/b/{self.cfg.nuvem_bucket}/o/"
               f"{urllib.parse.quote(nome, safe='')}")
        with self.s.get(url, params={"alt": "media"}, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(destino, "wb") as f:
                for pedaco in r.iter_content(1 << 20):
                    f.write(pedaco)

    def apagar(self, nome: str) -> None:
        url = (f"https://storage.googleapis.com/storage/v1/b/{self.cfg.nuvem_bucket}/o/"
               f"{urllib.parse.quote(nome, safe='')}")
        r = self.s.delete(url, timeout=60)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()


class TransporteLocal:
    """Nuvem simulada numa pasta (testes). `produtor(prefixo, marcas, pasta)` faz o papel do Job."""

    def __init__(self, raiz: pathlib.Path, produtor):
        self.raiz, self.produtor = pathlib.Path(raiz), produtor

    def disparar(self, prefixo, marcas):
        pasta = self.raiz / prefixo
        pasta.mkdir(parents=True, exist_ok=True)
        self.produtor(prefixo, marcas, pasta)

    def listar(self, prefixo):
        base = self.raiz / prefixo
        return [str(p.relative_to(self.raiz)).replace("\\", "/") for p in base.rglob("*") if p.is_file()] if base.exists() else []

    def baixar(self, nome, destino):
        shutil.copyfile(self.raiz / nome, destino)

    def apagar(self, nome):
        (self.raiz / nome).unlink(missing_ok=True)


# --------------------------------------------------------------------------------------------------------------
# Estado local da puxada (fora do banco e fora do Drive: é por PC)
# --------------------------------------------------------------------------------------------------------------
def _arq_estado(cfg) -> pathlib.Path:
    return pathlib.Path(cfg.pasta_logs) / "nuvem.json"


def ler_estado(cfg) -> dict:
    try:
        return json.loads(_arq_estado(cfg).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _gravar_estado(cfg, e: dict):
    p = _arq_estado(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(e, ensure_ascii=False, indent=1, default=str), encoding="utf-8")


def _agora():
    return datetime.datetime.now(datetime.timezone.utc)


# --------------------------------------------------------------------------------------------------------------
# Mescla no banco local
# --------------------------------------------------------------------------------------------------------------
def _qi(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def _colunas_locais(cur, tabela):
    """{coluna: (tipo formatado, gerada, identidade_always)} das colunas da tabela local."""
    cur.execute("""select a.attname, format_type(a.atttypid, a.atttypmod), a.attgenerated <> '', a.attidentity = 'a'
                   from pg_attribute a where a.attrelid = %s::regclass and a.attnum > 0 and not a.attisdropped""",
                (f"public.{_qi(tabela)}",))
    return {n: (t, g, i) for n, t, g, i in cur.fetchall()}


def _pk(cur, tabela):
    cur.execute("""select a.attname from pg_index i join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
                   where i.indrelid = %s::regclass and i.indisprimary""", (f"public.{_qi(tabela)}",))
    return [r[0] for r in cur.fetchall()]


def _carregar_csv(cur, tabela, info, arquivo, locais):
    """Carrega o CSV numa tabela temporária (sem restrições) e devolve (nome_tmp, colunas_úteis)."""
    cols_csv = info["colunas"]
    tmp = f"_nuvem_{tabela}"
    defs = ", ".join(f"{_qi(c)} {locais[c][0] if c in locais else 'text'}" for c in cols_csv)
    cur.execute(f"CREATE TEMP TABLE {_qi(tmp)} ({defs}) ON COMMIT DROP")
    with gzip.open(arquivo, "rt", encoding="utf-8", newline="") as f:
        cur.copy_expert(f"COPY {_qi(tmp)} ({', '.join(_qi(c) for c in cols_csv)}) FROM STDIN "
                        f"WITH (FORMAT csv, HEADER true, NULL '{NULO}')", f)
    proibidas = COLUNAS_PROIBIDAS.get(tabela, set())
    uteis = [c for c in cols_csv if c in locais and not locais[c][1] and c not in proibidas]
    ignoradas = [c for c in cols_csv if c not in uteis]
    if ignoradas:
        log.info("%s: colunas da nuvem ignoradas (inexistentes aqui, geradas ou proibidas): %s", tabela, ", ".join(ignoradas))
    return tmp, uteis


def _ids_locais_so_locais(cur, tabela, tmp, pk):
    cur.execute(f"select {_qi(pk)}::text from public.{_qi(tabela)} t where not exists "
                f"(select 1 from {_qi(tmp)} n where n.{_qi(pk)}::text = t.{_qi(pk)}::text) order by 1")
    return [r[0] for r in cur.fetchall()]


def _mesclar_retrato(cur, tabela, info, arquivo, relatorio):
    locais = _colunas_locais(cur, tabela)
    if not locais:
        raise NuvemErro(f"a tabela {tabela} não existe no banco local")
    pks = _pk(cur, tabela)
    if len(pks) != 1:
        raise NuvemErro(f"{tabela}: chave primária com {len(pks)} colunas (a recepção da nuvem espera exatamente 1)")
    pk = pks[0]
    tmp, uteis = _carregar_csv(cur, tabela, info, arquivo, locais)
    if pk not in uteis:
        raise NuvemErro(f"{tabela}: a coluna-chave {pk} não veio no arquivo da nuvem")
    cur.execute(f"select count(*) from {_qi(tmp)}")
    n = cur.fetchone()[0]
    if n != info["linhas"]:
        raise NuvemErro(f"{tabela}: o manifesto diz {info['linhas']} linhas, o arquivo tem {n}")

    cur.execute("select count(*) from sincronizacao.nuvem_ids where tabela = %s", (tabela,))
    ja_registradas = cur.fetchone()[0]
    if ja_registradas == 0:                                 # primeira importação: nada é apagado; só informa
        so_locais = _ids_locais_so_locais(cur, tabela, tmp, pk)
        relatorio[tabela] = {"linhas_so_locais": len(so_locais), "exemplos": so_locais[:50]}

    # exclusão: só o que veio da nuvem e não está mais no retrato
    cur.execute(f"""select i.id_local from sincronizacao.nuvem_ids i where i.tabela = %s
                      and not exists (select 1 from {_qi(tmp)} n where n.{_qi(pk)}::text = i.id_nuvem)""", (tabela,))
    sumiram = [r[0] for r in cur.fetchall()]
    if len(sumiram) > FREIO_MIN_LINHAS and len(sumiram) > FREIO_FRACAO * ja_registradas:
        raise NuvemErro(f"{tabela}: o retrato apagaria {len(sumiram)} de {ja_registradas} linhas vindas da nuvem; "
                        f"travado por segurança (confira o Job; para aceitar, apague o registro em sincronizacao.nuvem_ids)")
    apagadas = 0
    if sumiram:
        cur.execute(f"delete from public.{_qi(tabela)} where {_qi(pk)}::text = any(%s)", (sumiram,))
        apagadas = cur.rowcount
        cur.execute("delete from sincronizacao.nuvem_ids where tabela = %s and id_nuvem = any(%s)", (tabela, sumiram))

    # upsert: só escreve quando mudou (e só o que muda viaja ao outro PC)
    cols = ", ".join(_qi(c) for c in uteis)
    sets = ", ".join(f"{_qi(c)} = EXCLUDED.{_qi(c)}" for c in uteis if c != pk)
    novo = ", ".join(f"EXCLUDED.{_qi(c)}::text" for c in uteis if c != pk)
    velho = ", ".join(f"t.{_qi(c)}::text" for c in uteis if c != pk)
    ident = " OVERRIDING SYSTEM VALUE" if any(locais[c][2] for c in uteis) else ""
    if sets:
        acao = f"DO UPDATE SET {sets} WHERE ({velho}) IS DISTINCT FROM ({novo})"
    else:
        acao = "DO NOTHING"
    cur.execute(f"INSERT INTO public.{_qi(tabela)} AS t ({cols}){ident} SELECT {cols} FROM {_qi(tmp)} "
                f"ON CONFLICT ({_qi(pk)}) {acao}")
    gravadas = cur.rowcount                                 # inseridas + realmente alteradas
    cur.execute(f"""insert into sincronizacao.nuvem_ids (tabela, id_nuvem, id_local)
                    select %s, {_qi(pk)}::text, {_qi(pk)}::text from {_qi(tmp)} on conflict do nothing""", (tabela,))
    return {"linhas_nuvem": n, "escritas": gravadas, "apagadas": apagadas}


def _mesclar_incremental(cur, tabela, info, arquivo):
    locais = _colunas_locais(cur, tabela)
    if not locais:
        raise NuvemErro(f"a tabela {tabela} não existe no banco local")
    pks = _pk(cur, tabela)
    if len(pks) != 1:
        raise NuvemErro(f"{tabela}: chave primária com {len(pks)} colunas (a recepção da nuvem espera exatamente 1)")
    pk = pks[0]
    tmp, uteis = _carregar_csv(cur, tabela, info, arquivo, locais)
    cols_ins = [c for c in uteis if c != pk]
    cur.execute(f"select count(*) from {_qi(tmp)}")
    n = cur.fetchone()[0]
    if n != info["linhas"]:
        raise NuvemErro(f"{tabela}: o manifesto diz {info['linhas']} linhas, o arquivo tem {n}")
    cur.execute(f"select {_qi(pk)}::text from {_qi(tmp)} order by {_qi(pk)}::bigint")
    ids_nuvem = [r[0] for r in cur.fetchall()]
    cur.execute("select id_nuvem from sincronizacao.nuvem_ids where tabela = %s and id_nuvem = any(%s)", (tabela, ids_nuvem))
    existentes = {r[0] for r in cur.fetchall()}            # reimportação nunca duplica
    inseridas, cols = 0, ", ".join(_qi(c) for c in cols_ins)
    for idn in ids_nuvem:
        if idn in existentes:
            continue
        cur.execute(f"INSERT INTO public.{_qi(tabela)} ({cols}) SELECT {cols} FROM {_qi(tmp)} "
                    f"WHERE {_qi(pk)}::text = %s RETURNING {_qi(pk)}::text", (idn,))
        idl = cur.fetchone()[0]
        cur.execute("insert into sincronizacao.nuvem_ids (tabela, id_nuvem, id_local) values (%s, %s, %s)", (tabela, idn, idl))
        inseridas += 1
    if ids_nuvem:
        cur.execute("""insert into sincronizacao.nuvem_marcas (tabela, marca) values (%s, %s)
                       on conflict (tabela) do update set marca = greatest(sincronizacao.nuvem_marcas.marca, excluded.marca),
                                                          atualizado_em = now()""", (tabela, int(ids_nuvem[-1])))
    return {"linhas_nuvem": n, "escritas": inseridas, "apagadas": 0}


def _conferir_admin(cur) -> str | None:
    """Regra 9 do VAR: só alerta, nunca corrige."""
    cur.execute("select cargos from public.rag_usuarios where email = %s", (ADMIN_EMAIL,))
    r = cur.fetchone()
    if r is None:
        return f"{ADMIN_EMAIL} não existe em rag_usuarios"
    cargos = sorted(r[0] or [])
    if cargos != sorted(ADMIN_CARGOS):
        return f"{ADMIN_EMAIL} está com os cargos {cargos}; o esperado é {ADMIN_CARGOS}"
    return None


def mesclar(cfg, manifesto: dict, pasta: pathlib.Path, marcas: dict) -> dict:
    """Aplica o conteúdo baixado ao banco local, tudo ou nada. Devolve o resumo."""
    conn = psycopg2.connect(**cfg.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("select set_config('statement_timeout', '0', true)")
            resumo, relatorio, pendentes = {}, {}, {}
            tabs = manifesto["tabelas"]
            for t in tabs:
                if t not in ORDEM:
                    log.warning("tabela %s do manifesto não está na lista da recepção; ignorada", t)
            retratos = [t for t in ORDEM if t in tabs and tabs[t]["modo"] == "retrato"]
            incrementais = [t for t in ORDEM if t in tabs and tabs[t]["modo"] == "incremental"]
            # 1) exclusões dos filhos para os pais ficam dentro de cada tabela; a ordem de upsert respeita as FKs (pais antes)
            for t in retratos:
                resumo[t] = _mesclar_retrato(cur, t, tabs[t], pasta / tabs[t]["arquivo"], relatorio)
                if tabs[t].get("orfaos"):
                    resumo[t]["orfaos_na_nuvem"] = tabs[t]["orfaos"]
            for t in incrementais:
                if t not in marcas:                                       # primeira vez: o usuário define a marca
                    pendentes[t] = {"max_id_nuvem": tabs[t].get("max_id"), "linhas_nuvem": tabs[t].get("linhas_total")}
                    continue
                if not tabs[t].get("arquivo"):
                    resumo[t] = {"linhas_nuvem": 0, "escritas": 0, "apagadas": 0}
                    continue
                resumo[t] = _mesclar_incremental(cur, t, tabs[t], pasta / tabs[t]["arquivo"])
            alerta_admin = _conferir_admin(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"tabelas": resumo, "relatorio_primeira_importacao": relatorio, "marcas_pendentes": pendentes,
            "alerta_admin": alerta_admin}


# --------------------------------------------------------------------------------------------------------------
# Orquestração
# --------------------------------------------------------------------------------------------------------------
def ler_marcas(cfg) -> dict:
    conn = psycopg2.connect(**cfg.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("select tabela, marca from sincronizacao.nuvem_marcas")
            return {t: m for t, m in cur.fetchall()}
    finally:
        conn.close()


def definir_marca(cfg, tabela: str, marca: int):
    """Define (ou move) a marca de uma tabela só de inserção. Use o maior id da nuvem que o banco local JÁ tem."""
    if tabela not in ORDEM:
        raise ValueError(f"tabela desconhecida: {tabela}")
    conn = psycopg2.connect(**cfg.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("""insert into sincronizacao.nuvem_marcas (tabela, marca) values (%s, %s)
                           on conflict (tabela) do update set marca = excluded.marca, atualizado_em = now()""", (tabela, marca))
        conn.commit()
    finally:
        conn.close()


def _validar_manifesto(m: dict):
    if m.get("versao") != VERSAO_CONTRATO:
        raise NuvemErro(f"versão do manifesto {m.get('versao')!r} diferente da esperada ({VERSAO_CONTRATO}): atualize o código")
    if not isinstance(m.get("tabelas"), dict):
        raise NuvemErro("manifesto sem 'tabelas'")
    for t, i in m["tabelas"].items():
        if i.get("modo") not in ("retrato", "incremental"):
            raise NuvemErro(f"{t}: modo inválido {i.get('modo')!r}")
        if i.get("arquivo"):
            for k in ("sha256", "linhas", "colunas"):
                if k not in i:
                    raise NuvemErro(f"{t}: manifesto sem '{k}'")
        elif i["modo"] == "retrato":
            raise NuvemErro(f"{t}: retrato sem arquivo")


def _sha256(caminho: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for p in iter(lambda: f.read(1 << 20), b""):
            h.update(p)
    return h.hexdigest()


def puxar(cfg, transporte=None, forcar: bool = False) -> dict | None:
    """Uma puxada completa. Nunca levanta exceção: devolve o resumo, ou {'erro': ...}; None se desligada/fora de hora."""
    if not cfg.puxar_nuvem:
        return None
    est = ler_estado(cfg)
    agora = _agora()
    if not forcar and est.get("ultima_tentativa"):
        ultima = datetime.datetime.fromisoformat(est["ultima_tentativa"])
        if agora - ultima < datetime.timedelta(minutes=cfg.nuvem_intervalo_min):
            return None
    est["ultima_tentativa"] = agora.isoformat()
    execucao = f"{agora:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"
    prefixo = f"{cfg.nuvem_prefixo.strip('/')}/{execucao}/"
    tmp = pathlib.Path(cfg.pasta_logs) / "nuvem_tmp" / execucao
    transporte_ok = None
    try:
        transporte = transporte or TransporteGCP(cfg)
        transporte_ok = transporte
        marcas = ler_marcas(cfg)
        transporte.disparar(prefixo, marcas)
        nomes = transporte.listar(prefixo)
        if f"{prefixo}manifesto.json" not in nomes:
            raise NuvemErro("a nuvem não gravou o manifesto.json (execução incompleta)")
        tmp.mkdir(parents=True, exist_ok=True)
        transporte.baixar(f"{prefixo}manifesto.json", tmp / "manifesto.json")
        manifesto = json.loads((tmp / "manifesto.json").read_text(encoding="utf-8"))
        _validar_manifesto(manifesto)
        for t, i in manifesto["tabelas"].items():
            if not i.get("arquivo"):
                continue
            if f"{prefixo}{i['arquivo']}" not in nomes:
                raise NuvemErro(f"{t}: arquivo {i['arquivo']} ausente no bucket")
            transporte.baixar(f"{prefixo}{i['arquivo']}", tmp / i["arquivo"])
            if _sha256(tmp / i["arquivo"]) != i["sha256"]:
                raise NuvemErro(f"{t}: sha256 do arquivo diferente do manifesto (download corrompido)")
        r = mesclar(cfg, manifesto, tmp, marcas)
    except Exception as e:
        log.exception("falha na puxada da nuvem")
        est["falhando_desde"] = est.get("falhando_desde") or agora.isoformat()
        est["ultimo_erro"] = f"{type(e).__name__}: {e}"
        _gravar_estado(cfg, est)
        desde = datetime.datetime.fromisoformat(est["falhando_desde"])
        if agora - desde > datetime.timedelta(hours=cfg.nuvem_falha_alerta_h):
            avisos.avisar(cfg, "nuvem", "Sincronização BD: nuvem falhando",
                          f"A puxada da nuvem falha desde {desde:%d/%m %H:%M}. Último erro: {est['ultimo_erro']}"[:400])
        shutil.rmtree(tmp, ignore_errors=True)
        return {"erro": est["ultimo_erro"]}
    # sucesso: limpeza do bucket (falha aqui não desfaz a importação; a regra de 3 dias do bucket cobre)
    try:
        for n in transporte_ok.listar(prefixo):
            transporte_ok.apagar(n)
    except Exception:
        log.warning("não consegui apagar a pasta %s do bucket (a regra de ciclo de vida apaga em 3 dias)", prefixo)
    shutil.rmtree(tmp, ignore_errors=True)
    est.update({"ultima_ok": agora.isoformat(), "falhando_desde": None, "ultimo_erro": None, "ultimo_resumo": r["tabelas"]})
    if r["marcas_pendentes"]:
        est["marcas_pendentes"] = r["marcas_pendentes"]
    else:
        est.pop("marcas_pendentes", None)
    _gravar_estado(cfg, est)
    avisos.limpar(cfg, "nuvem")
    if r["relatorio_primeira_importacao"]:
        arq = pathlib.Path(cfg.pasta_logs) / f"nuvem_somente_local_{agora:%Y%m%dT%H%M%S}.json"
        arq.write_text(json.dumps(r["relatorio_primeira_importacao"], ensure_ascii=False, indent=1), encoding="utf-8")
        total = sum(v["linhas_so_locais"] for v in r["relatorio_primeira_importacao"].values())
        avisos.avisar(cfg, "nuvem-primeira", "Sincronização BD: primeira importação da nuvem",
                      f"{total} linha(s) locais não existem na nuvem (nada foi apagado). Relatório: {arq}")
    if r["marcas_pendentes"]:
        avisos.avisar(cfg, "nuvem-marcas", "Sincronização BD: defina as marcas da nuvem",
                      "Tabelas só de inserção sem marca: " + ", ".join(r["marcas_pendentes"]) +
                      ". Use: python -m sincronizador nuvem-marca <tabela> <maior id da nuvem que já existe aqui>")
    if r["alerta_admin"]:
        avisos.avisar(cfg, "nuvem-admin", "Sincronização BD: cargos do administrador", r["alerta_admin"])
    else:
        avisos.limpar(cfg, "nuvem-admin")
    log.info("puxada da nuvem concluída: %s", json.dumps(r["tabelas"], ensure_ascii=False))
    return r
