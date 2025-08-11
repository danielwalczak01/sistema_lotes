def obter_query_lote(ultima_data):
    return f"""
    SELECT DISTINCT 
               lc.cd_lote,
               c.nm_cidade AS cidade_origem,
               c2.nm_cidade AS cidade_destino,
               lc.dt_lote,
               lcv.vl_tarifa_empresa as "Frete Empresa",
               lcv.vl_tarifa_motorista as "Frete Motorista",
               p2.nm_pessoa as "Pagador",
               lc.dt_fechamento,
               lcp.cd_produto,
               lcp.cd_unidade,
               lcp.qt_produto,
               a.nm_agencia,
               p2.nm_pessoa AS cliente,
               CASE
                   WHEN lc.ds_lote IS NULL THEN 'Sem Observações'
                   ELSE lc.ds_lote
               END AS ds_observacao,
               CASE
                   WHEN coc.dt_inicio IS NULL THEN '9999-12-31'
                   ELSE coc.dt_inicio
               END AS dt_inicio,
               CASE
                   WHEN coc.dt_termino IS NULL THEN '9999-12-31'
                   ELSE coc.dt_termino
               END AS dt_termino,
               CASE
                   WHEN lc.id_situacao = 2 THEN 'Em Andamento'
                   WHEN lc.id_situacao = 1 THEN 'Suspenso'
                   WHEN lc.id_situacao = 3 THEN 'Suspenso para Cte'
                   WHEN lc.id_situacao = 0 THEN 'Encerrado'
               END AS situacao,
               CASE
                   WHEN lc.dt_atualizacao IS NULL THEN lc.dt_lote
                   ELSE lc.dt_atualizacao
               END AS dt_atualizacao,
               lc.ds_observacao_lote_carregamento AS saldo,
               lc.ds_observacao_ordem_carregamento as rota,
               lcv.vl_tarifa_empresa as valor,
               p3.nm_pessoa as criado_alterado,
               lc.ds_observacao_interna,
               cc.vl_credito,
               cc.vl_saldo,
               pc.nr_telefone
        FROM lote_carregamento lc
        LEFT JOIN lote_carregamento_produto lcp ON lcp.cd_lote = lc.cd_lote
        LEFT JOIN lote_carregamento_agencia lca ON lca.cd_lote = lc.cd_lote
        LEFT JOIN lote_carregamento_valor lcv ON lcv.cd_lote = lc.cd_lote 
        LEFT JOIN pessoa p ON p.cd_pessoa = lc.cd_pessoa_filial
        LEFT JOIN pessoa p2 ON p2.cd_pessoa = lc.cd_pessoa_pagador
        LEFT JOIN pessoa p3 ON p3.cd_pessoa = lc.cd_pessoa_usuario 
        LEFT JOIN cliente_observacao_ctrc coc ON coc.cd_pessoa = p2.cd_pessoa
        LEFT JOIN cidade c ON c.cd_cidade = lc.cd_cidade_origem
        LEFT JOIN cidade c2 ON c2.cd_cidade = lc.cd_cidade_destino
        LEFT JOIN cliente_credito cc ON cc.cd_pessoa = p2.cd_pessoa
        LEFT JOIN pessoa_contato pc ON pc.cd_pessoa = p2.cd_pessoa
        INNER JOIN agencia a ON a.cd_agencia = lc.cd_agencia
        WHERE 
            CASE
                WHEN lc.dt_atualizacao IS NULL THEN lc.dt_lote
                ELSE lc.dt_atualizacao
            END >= '{ultima_data}'
            AND (
                lc.id_situacao IN (1, 2, 3)
                OR (lc.id_situacao = 0 AND (lc.dt_fechamento IS NULL OR lc.dt_fechamento >= '2024-08-01'))
            )
    """

def obter_query_agencia():
    return "SELECT cd_agencia, nm_agencia FROM agencia"

def obter_query_liberacao(ultima_data):
    return f"""
    SELECT DISTINCT CONCAT(lca.cd_agencia, '-', lca.cd_lote) AS agencia_lote,
                    lca.cd_agencia as agencia,
                    lca.cd_lote
    FROM lote_carregamento_agencia lca
    LEFT JOIN lote_carregamento lc ON lc.cd_lote = lca.cd_lote
    WHERE lc.dt_atualizacao >= '{ultima_data}'
    """
