"""Medicao de desempenho (nao e teste de correcao): publicar e aplicar uma carga. Tamanho: variavel SINC_BENCH_N (padrao 20000)."""
import os
import time

from sincronizador import aplicador, publicador


def test_carga(par):
    n = int(os.environ.get("SINC_BENCH_N", "20000"))
    par.sql("casa", f"insert into pessoa (nome, idade, obs) select 'n' || g, g % 90, repeat('z', 150) from generate_series(1, {n}) g")
    t0 = time.time()
    publicador.publicar(par.cfg["casa"])
    t1 = time.time()
    a = aplicador.aplicar(par.cfg["cnpq"])
    t2 = time.time()
    print(f"\nINSERT {n}: publicar {t1 - t0:.1f}s ({n / max(t1 - t0, 0.01):.0f}/s) | aplicar {t2 - t1:.1f}s ({n / max(t2 - t1, 0.01):.0f}/s)")
    assert a["mudancas"] == n
    par.sql("casa", "update pessoa set idade = (idade + 1) % 90")
    t0 = time.time()
    publicador.publicar(par.cfg["casa"])
    t1 = time.time()
    aplicador.aplicar(par.cfg["cnpq"])
    t2 = time.time()
    print(f"UPDATE {n}: publicar {t1 - t0:.1f}s | aplicar {t2 - t1:.1f}s ({n / max(t2 - t1, 0.01):.0f}/s)")
