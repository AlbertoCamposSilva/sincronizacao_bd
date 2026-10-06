"""Linha de comando:  python -m sincronizador <comando>   (use pythonw.exe nas tarefas agendadas: sem janela)."""
import argparse
import datetime
import json
import sys

from . import config, estado, lote


def _cfg():
    return config.carregar()


def cmd_gerar_chave(a):
    print(lote.gerar_chave())
    print("Guarde este valor no cofre (acs-toolbox) como SYNC_BD_CHAVE, igual nos dois PCs. Não o salve em arquivo do projeto.",
          file=sys.stderr)


def cmd_inicializar(a):
    from . import instalar
    cfg = _cfg()
    for sub in (cfg.saida, cfg.saida_do_par):
        sub.mkdir(parents=True, exist_ok=True)
    marcador = cfg.pasta / config.MARCADOR
    if not marcador.exists():
        marcador.write_text(json.dumps({"protocolo": lote.VERSAO_PROTOCOLO, "nos": list(config.NOS),
                                        "criado_em": estado.agora_iso()}, indent=1), encoding="utf-8")
    r = instalar.instalar(cfg)
    print(f"Pasta do Drive: {cfg.pasta}")
    print(json.dumps(r, ensure_ascii=False, indent=1))
    print("Instalado. Próximo: gerar a cópia física para o outro PC (ver documentacao/IMPLANTACAO_PC_CNPQ.md).")


def cmd_preflight(a):
    """Verificação somente leitura do banco: o que atrapalharia a instalação."""
    from . import preflight
    cfg = config.carregar(exigir_chave=False)
    sinal = {"ok": "[ok]    ", "info": "[info]  ", "aviso": "[AVISO] ", "erro": "[ERRO]  "}
    r = preflight.auditar(cfg)
    for nivel, msg in r:
        print(sinal[nivel] + msg)
    if any(n == "erro" for n, _ in r):
        sys.exit(2)


def cmd_ciclo(a):
    from . import ciclo
    r = ciclo.executar_ciclo(_cfg())
    if sys.stdout is not None and sys.stdout.isatty():
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str))


def cmd_status(a):
    cfg = _cfg()
    for rotulo, caminho in ((cfg.no, cfg.estado_proprio), (cfg.par, cfg.estado_do_par)):
        e = estado.ler(caminho)
        print(f"== {rotulo} ==")
        if not e:
            print("   (sem estado)")
            continue
        for k in ("atualizado_em", "pausado", "publicado_seq", "aplicou_do_par", "parado", "lacuna", "ultimo_erro",
                  "wal_retido_bytes", "conflitos_total", "ultima_comparacao"):
            if e.get(k) not in (None, False):
                print(f"   {k}: {e.get(k)}")
        if e.get("mensagem_parado"):
            print(f"   >>> PARADO: {e['mensagem_parado']}")


def cmd_comparar(a):
    from . import comparar
    r = comparar.executar(_cfg())
    print(json.dumps(r, ensure_ascii=False, indent=1))


def cmd_pausar(a):
    cfg = _cfg()
    (cfg.pasta / "PAUSAR").write_text(estado.agora_iso(), encoding="utf-8")
    print("Sincronização pausada nos dois PCs (arquivo PAUSAR no Drive). Use 'retomar' para voltar.")


def cmd_retomar(a):
    cfg = _cfg()
    (cfg.pasta / "PAUSAR").unlink(missing_ok=True)
    print("Sincronização retomada.")


def cmd_ddl(a):
    """Executa um DDL que o fluxo não replica sozinho e o enfileira para o par, como DDL simples."""
    import psycopg2
    cfg = _cfg()
    conn = psycopg2.connect(**cfg.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("select set_config('sincronizacao.aplicando', 'on', true)")
            cur.execute(a.comando)
            cur.execute("select sincronizacao.registrar_ddl(%s, %s, false, null)", (a.comando.split()[0].upper(), a.comando))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print("Executado aqui e enfileirado para o par.")


def cmd_ddl_resolvido(a):
    """Marca um DDL (id mostrado na mensagem de fluxo parado) como já aplicado à mão neste banco."""
    import psycopg2
    cfg = _cfg()
    conn = psycopg2.connect(**cfg.dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("insert into sincronizacao.ddl_resolvidos (id) values (%s::uuid) on conflict do nothing", (a.id,))
    conn.close()
    print("Registrado. O próximo ciclo pula esse DDL e segue.")


def cmd_instalar_tarefas(a):
    from . import tarefas
    script = tarefas.script_powershell(minutos=a.minutos)
    if a.dry_run:
        print(script)
        return
    print(tarefas.executar_powershell(script))


def cmd_remover_tarefas(a):
    from . import tarefas
    print(tarefas.executar_powershell(tarefas.script_remocao()))


def main(argv=None):
    p = argparse.ArgumentParser(prog="sincronizador", description="Sincronização bidirecional casa <-> CNPq via Google Drive")
    sp = p.add_subparsers(dest="cmd", required=True)
    for nome, fn, ajuda in (("gerar-chave", cmd_gerar_chave, "gera a chave de criptografia dos lotes"),
                            ("inicializar", cmd_inicializar, "cria a pasta do Drive e instala a sincronização no banco"),
                            ("preflight", cmd_preflight, "verifica o banco (somente leitura) antes de instalar"),
                            ("ciclo", cmd_ciclo, "um ciclo (publica + aplica); é o que a tarefa agendada roda"),
                            ("status", cmd_status, "estado dos dois nós"),
                            ("comparar", cmd_comparar, "impressão digital dos bancos e conferência com o par"),
                            ("pausar", cmd_pausar, "pausa a sincronização nos dois PCs"),
                            ("retomar", cmd_retomar, "retoma a sincronização"),
                            ("remover-tarefas", cmd_remover_tarefas, "remove as tarefas do Agendador")):
        sp.add_parser(nome, help=ajuda).set_defaults(fn=fn)
    d = sp.add_parser("ddl", help="executa um DDL aqui e o enfileira para o par")
    d.add_argument("comando")
    d.set_defaults(fn=cmd_ddl)
    r = sp.add_parser("ddl-resolvido", help="marca um DDL como já aplicado manualmente neste banco (retoma o fluxo)")
    r.add_argument("id")
    r.set_defaults(fn=cmd_ddl_resolvido)
    t = sp.add_parser("instalar-tarefas", help="instala as tarefas no Agendador (sem janela, sem administrador)")
    t.add_argument("--minutos", type=int, default=10)
    t.add_argument("--dry-run", action="store_true", help="só mostra o script PowerShell")
    t.set_defaults(fn=cmd_instalar_tarefas)
    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
