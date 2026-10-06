# Backup particionado (cópia do banco em partes de 1 GB pelo Google Drive)

Método usado na carga inicial (set/2026): o banco (`E:\PostgreSQL` + `E:\Programas`) foi compactado em ~122 volumes de 1 GB
(`PostgreSQL_completo_backup.partNNN.tar.gz`, ~121 GB), enviado ao Google Drive e restaurado no outro PC. **Guardado para uso
futuro** (por exemplo, montar um terceiro PC sem pendrive). Em out/2026 a cópia passou a ser feita por pendrive e robocopy.

- `gerar_backup_particionado.py`: gera as partes (Drive `J:\Meu Drive\PostgreSQL`, pausa se o cache do Drive encher o `C:`).
  Empacota também as pastas vazias do PostgreSQL (`pg_notify`, `pg_replslot`, `pg_tblspc`...), sem as quais o servidor não sobe.
- `restaurar_backup_particionado.py`: junta as partes e extrai em `E:\`, recriando as 12 pastas estruturais obrigatórias.
  Procura as partes em `N:`, `J:`, `G:` ou `D:\Meu Drive\PostgreSQL`, ou use `BACKUP_ORIGEM_DIR` / um argumento.

Uso: `python gerar_backup_particionado.py` (origem) e `python restaurar_backup_particionado.py [pasta_das_partes]` (destino).
Dependências: `tqdm` e `acs-toolbox`.

**Atenção (LGPD):** as partes contêm o banco inteiro, com dados pessoais, **sem criptografia**. Se usar o método de novo, apague
as partes do Drive logo depois da restauração (foi o que se fez em 06/10/2026). Para um novo clone, a cópia física precisa ser feita
com o PostgreSQL desligado e, depois, o outro PC roda o `inicializar` do sincronizador (ver `../../IMPLANTACAO_PC_CNPQ.md`).
