const amqplib = require('amqplib');
const config = require('./config');

async function startReceiver() {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    

    await channel.assertExchange(config.RESULT_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    

    await channel.assertQueue(config.RESULT_QUEUE, { durable: true });
    await channel.bindQueue(config.RESULT_QUEUE, config.RESULT_EXCHANGE, 'result');
    await channel.bindQueue(config.RESULT_QUEUE, config.RESULT_EXCHANGE, 'all-result'); 
    

    console.log('Client récepteur connecté à RabbitMQ');
    console.log(`En attente de résultats sur ${config.RESULT_QUEUE}...`);
    

    channel.consume(config.RESULT_QUEUE, (msg) => {

      if (msg !== null) {

        try {

          const result = JSON.parse(msg.content.toString());
          
          let operationSymbol;

          switch(result.op) {
            
            case config.OPERATIONS.ADD:
              operationSymbol = '+';
              break;

            case config.OPERATIONS.SUB:
              operationSymbol = '-';
              break;

            case config.OPERATIONS.MUL:
              operationSymbol = '*';
              break;

            case config.OPERATIONS.DIV:
              operationSymbol = '/';
              break;

            default:
              operationSymbol = '?';

          }

          
          console.log('┌─────────────────────────────────────────────────┐');
          console.log('│ RÉSULTAT DE CALCUL                              │');
          console.log('├─────────────────────────────────────────────────┤');
          console.log(` Opération: ${result.n1} ${operationSymbol} ${result.n2}${' '.repeat(Math.max(0, 34 - (result.n1.toString().length + result.n2.toString().length + operationSymbol.length)))}`);
          console.log(` Résultat:  ${result.result}${' '.repeat(Math.max(0, 41 - result.result.toString().length))}`);
          

          if (result.allOperation) {
            console.log(' (Partie de l\'opération ALL)                     ');
          }

          
          console.log('└─────────────────────────────────────────────────┘');
          
          channel.ack(msg);

        } catch (error) {

          console.error('Erreur lors du traitement du résultat:', error);
          channel.nack(msg);

        }

      }

    });

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }
  
}

startReceiver();