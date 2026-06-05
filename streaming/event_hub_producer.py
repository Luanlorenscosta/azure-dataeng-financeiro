# ================================================================
# event_hub_producer.py
# Simula transações financeiras em tempo real via Kafka
# Equivalente ao: Azure Event Hub Producer
# O que faz:
#   - Gera transações financeiras sintéticas
#   - Envia para o tópico Kafka "transacoes-financeiras"
#   - Simula o app do banco enviando eventos em tempo real
# Uso: python streaming/event_hub_producer.py
# ================================================================

import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from loguru import logger

fake = Faker("pt_BR")
random.seed()

# ----------------------------------------------------------------
# Configuração do Kafka
# Equivalente ao: Event Hub connection string no Azure
# ----------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"
TOPICO       = "transacoes-financeiras"
INTERVALO_SEGUNDOS = 0.5  # uma transação a cada 0.5 segundos

# ----------------------------------------------------------------
# Dados de referência simulados
# ----------------------------------------------------------------
TIPOS        = ["pix", "debito", "credito", "ted", "boleto"]
STATUS       = ["aprovada", "aprovada", "aprovada", "negada", "suspeita"]
CANAIS       = ["app", "app", "web", "pos", "atm"]
ESTADOS      = ["SP", "RJ", "MG", "BA", "RS", "PR", "PE", "CE", "GO", "SC"]
IDS_CONTA    = list(range(1, 1018))

def gerar_transacao() -> dict:
    """
    Gera uma transação financeira sintética.
    Cada transação é um evento — equivalente a uma mensagem no Event Hub.
    """
    is_fraude    = random.random() < 0.03  # 3% de chance de fraude
    valor        = round(random.uniform(500, 20000), 2) if is_fraude \
                   else round(random.uniform(1, 3000), 2)
    score_fraude = round(random.uniform(0.7, 1.0), 4) if is_fraude \
                   else round(random.uniform(0.0, 0.3), 4)

    return {
        "evento_id":        fake.uuid4(),
        "timestamp":        datetime.now().isoformat(),
        "id_conta":         random.choice(IDS_CONTA),
        "valor":            valor,
        "tipo":             random.choice(TIPOS),
        "status":           "suspeita" if is_fraude else random.choice(STATUS),
        "canal":            random.choice(CANAIS),
        "estado_origem":    random.choice(ESTADOS),
        "is_fraude":        is_fraude,
        "score_fraude":     score_fraude,
        "ip_origem":        fake.ipv4(),
        "device_id":        fake.uuid4(),
        "comerciante":      fake.company(),
        "descricao":        fake.sentence(nb_words=3),
    }

def main():
    logger.info("=" * 55)
    logger.info("KAFKA PRODUCER — TRANSAÇÕES EM TEMPO REAL")
    logger.info("Equivalente: Azure Event Hub Producer")
    logger.info(f"Broker: {KAFKA_BROKER} | Tópico: {TOPICO}")
    logger.info("=" * 55)

    # Conecta ao Kafka
    logger.info("Conectando ao Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    logger.success("Conectado! Enviando transações... (Ctrl+C para parar)")
    logger.info("")

    enviados  = 0
    fraudes   = 0

    try:
        while True:
            transacao = gerar_transacao()

            # Envia para o Kafka
            # A chave é o id_conta — garante que transações da mesma conta
            # vão para a mesma partição (ordenação garantida por conta)
            producer.send(
                topic=TOPICO,
                key=str(transacao["id_conta"]),
                value=transacao,
            )

            enviados += 1
            if transacao["is_fraude"]:
                fraudes += 1
                logger.warning(
                    f"🚨 FRAUDE #{fraudes} | "
                    f"Conta {transacao['id_conta']} | "
                    f"R$ {transacao['valor']:,.2f} | "
                    f"Score: {transacao['score_fraude']} | "
                    f"{transacao['estado_origem']}"
                )
            else:
                logger.info(
                    f"✅ #{enviados:04d} | "
                    f"Conta {transacao['id_conta']:04d} | "
                    f"{transacao['tipo']:<8} | "
                    f"R$ {transacao['valor']:>8,.2f} | "
                    f"{transacao['canal']:<4} | "
                    f"{transacao['estado_origem']}"
                )

            # Flush a cada 10 mensagens para garantir entrega
            if enviados % 10 == 0:
                producer.flush()

            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        logger.info(f"\nProducer parado pelo usuário.")
        logger.success(f"Total enviado: {enviados} transações | {fraudes} fraudes")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
