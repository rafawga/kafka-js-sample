const Kafka = require('kafka-node');
const fs = require('fs');

// Configurações do TLS
const tlsOptions = {
    ca: [fs.readFileSync('U:/BDO/kafka/certificates/Bosch-CA-bundle.cert.pem')],
    cert: fs.readFileSync('U:/BDO/kafka/certificates/campinas-testing.cert.pem'),
    key: fs.readFileSync('U:/BDO/kafka/certificates/campinas-testing.key.pem'),
    passphrase: 'MBjwT7KwdZjavW3b',
};

// Configurações de conexão com o Kafka Broker
const kafkaHosts = [
    'ca0vm00191.br.bosch.com:9093',
    'ca0vm00192.br.bosch.com:9093',
    'ca0vm00193.br.bosch.com:9093',
];
const kafkaClientOptions = {
    kafkaHost: kafkaHosts.join(','),
    ssl: true,
    sslOptions: tlsOptions,
};

// Cria um cliente Kafka
const client = new Kafka.KafkaClient(kafkaClientOptions);

// Cria um produtor Kafka
const producer = new Kafka.Producer({ kafkaClient: client });

// Variável global para armazenar mensagens perdidas
context.buffer = [];

// Função para enviar dados ao Kafka Producer
function sendToKafka(data) {
    const kafkaMessage = {
        topic: 'br_ct_ico_test_general',
        messages: [JSON.stringify(data)],
    };

    producer.send([kafkaMessage], (error, result) => {
        if (error) {
            context.buffer.push(data);
            node.error(error); // Adiciona um log de erro
        } else {
            node.log('Mensagem enviada com sucesso para o Kafka'); // Adiciona um log de sucesso
        }
    });
}

// Função para envio periódico para o Kafka
function periodicSendToKafka() {
    // Obtém os dados da mensagem de entrada
    const data = { teste: 'teste' };

    // Chama a função para enviar os dados ao Kafka Producer
    sendToKafka(data);

    // Agende o próximo envio após 1 segundo
    setTimeout(periodicSendToKafka, 1000);
}

// Inicia o envio periódico para o Kafka
periodicSendToKafka();

// Continue o fluxo para os próximos nós
return msg;
