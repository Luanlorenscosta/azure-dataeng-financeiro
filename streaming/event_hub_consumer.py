# ================================================================
# event_hub_consumer.py
# Consome transações do Kafka e detecta fraudes em tempo real
# Equivalente ao: Azure Event Hub Consumer + Stream Analytics
# O que faz:
#   - Lê transações do tópico Kafka em tempo real
#   - Aplica regras de detecção de fraude
#   - Grava alertas em arquivo JSON
#   - Exibe dashboard em tempo real no terminal
# Uso: python streaming/event_hub_consumer.py
# ================================================================

import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from kafka import KafkaConsumer
from loguru import logger

# ----------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------
KAFKA_BROKER  = "localhost:9092"
TOPICO        = "transacoes-financeiras"
GROUP_ID      = "antifraude-consumer-group"
ALERTAS_PATH  = Path("data_lake/streaming/alertas")

# Regras de detecção de fraude em tempo real
# Equivalente às queries do Azure Stream Analytics
LIMITE_VALOR_SUSPEITO   = 5000.00   # transações acima desse valor são suspeitas
LIMITE_SCORE_FRAUDE     = 0.65      # score acima desse limite gera alerta
MAX_TRANSACOES_MINUTO   = 5         # mais que X transações por minuto por conta

# ----------------------------------------------------------------
# Estado em memória (equivalente à janela temporal do Stream Analytics)
# ----------------------------------------------------------------
transacoes_por_conta = defaultdict(list)   # histórico por conta
alertas_gerados      = []

# ----------------------------------------------------------------
# Regras de fraude
# ----------------------------------------------------------------
def avaliar_fraude(transacao: dict) -> list:
    """
    Aplica regras de detecção de fraude em tempo real.
    Retorna lista de alertas gerados (pode ser vazia).

    Equivalente às queries de janela temporal no Stream Analytics:
    SELECT * FROM transacoes
    WHERE score_fraude > 0.65
    OR valor > 5000
    """
    alertas = []
    id_conta = transacao["id_conta"]
    agora    = datetime.now()

    # Regra 1: Score de fraude alto
    if transacao["score_fraude"] > LIMITE_SCORE_FRAUDE:
        alertas.append({
            "regra":     "SCORE_FRAUDE_ALTO",
            "descricao": f"Score {transacao['score_fraude']} acima do limite {LIMITE_SCORE_FRAUDE}",
            "severidade": "ALTA",
        })

    # Regra 2: Valor muito alto
    if transacao["valor"] > LIMITE_VALOR_SUSPEITO:
        alertas.append({
            "regra":     "VALOR_SUSPEITO",
            "descricao": f"Transação de R$ {transacao['valor']:,.2f} acima do limite",
            "severidade": "MEDIA",
        })

    # Regra 3: Muitas transações em pouco tempo (velocity check)
    historico = transacoes_por_conta[id_conta]
    historico.append(agora)
    # Mantém só os últimos 60 segundos
    transacoes_por_conta[id_conta] = [
        t for t in historico
        if (agora - t).seconds < 60
    ]
    if len(transacoes_por_conta[id_conta]) > MAX_TRANSACOES_MINUTO:
        alertas.append({
            "regra":     "VELOCITY_CHECK",
            "descricao": f"Conta {id_conta} com {len(transacoes_por_conta[id_conta])} transações no último minuto",
            "severidade": "ALTA",
        })

    # Regra 4: Status já marcado como suspeito na origem
    if transacao["status"] == "suspeita":
        alertas.append({
            "regra":     "STATUS_SUSPEITA",
            "descricao": "Transação marcada como suspeita pelo sistema de origem",
            "severidade": "ALTA",
        })

    return alertas


def gravar_alerta(transacao: dict, alertas: list):
    """Grava alertas de fraude em arquivo JSON para análise posterior."""
    ALERTAS_PATH.mkdir(parents=True, exist_ok=True)

    alerta = {
        "timestamp_alerta": datetime.now().isoformat(),
        "evento_id":        transacao["evento_id"],
        "id_conta":         transacao["id_conta"],
        "valor":            transacao["valor"],
        "tipo":             transacao["tipo"],
        "estado":           transacao["estado_origem"],
        "score_fraude":     transacao["score_fraude"],
        "alertas":          alertas,
        "transacao":        transacao,
    }

    arquivo = ALERTAS_PATH / f"alerta_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(alerta, f, indent=2, ensure_ascii=False)

    alertas_gerados.append(alerta)


def main():
    logger.info("=" * 55)
    logger.info("KAFKA CONSUMER — DETECÇÃO DE FRAUDES EM TEMPO REAL")
    logger.info("Equivalente: Azure Event Hub + Stream Analytics")
    logger.info(f"Broker: {KAFKA_BROKER} | Tópico: {TOPICO}")
    logger.info(f"Consumer Group: {GROUP_ID}")
    logger.info("=" * 55)
    logger.info("Aguardando mensagens... (Ctrl+C para parar)")
    logger.info("")

    # Conecta ao Kafka como consumer
    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # lê só mensagens novas
        enable_auto_commit=True,
    )

    processadas = 0
    total_alertas = 0

    try:
        for mensagem in consumer:
            transacao = mensagem.value
            processadas += 1

            # Avalia regras de fraude
            alertas = avaliar_fraude(transacao)

            if alertas:
                total_alertas += 1
                gravar_alerta(transacao, alertas)

                # Exibe alerta no terminal
                regras = " | ".join(a["regra"] for a in alertas)
                logger.warning(
                    f"🚨 ALERTA #{total_alertas} | "
                    f"Conta {transacao['id_conta']:04d} | "
                    f"R$ {transacao['valor']:>8,.2f} | "
                    f"{regras}"
                )
            else:
                logger.info(
                    f"✅ #{processadas:04d} | "
                    f"Conta {transacao['id_conta']:04d} | "
                    f"{transacao['tipo']:<8} | "
                    f"R$ {transacao['valor']:>8,.2f} | OK"
                )

            # Dashboard a cada 50 mensagens
            if processadas % 50 == 0:
                logger.info(
                    f"\n📊 Dashboard: {processadas} processadas | "
                    f"{total_alertas} alertas | "
                    f"Taxa: {total_alertas/processadas*100:.1f}%\n"
                )

    except KeyboardInterrupt:
        logger.info(f"\nConsumer parado.")
        logger.success(f"Total processado: {processadas} | Alertas: {total_alertas}")
        consumer.close()

if __name__ == "__main__":
    main()
