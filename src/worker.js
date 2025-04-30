const amqplib = require('amqplib');
const config = require('./config');


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


function getRandomDelay() {

  return Math.floor(Math.random() * 
    (config.MAX_PROCESSING_TIME - config.MIN_PROCESSING_TIME + 1)) + 
    config.MIN_PROCESSING_TIME;

}


function performOperation(n1, n2, op = config.OPERATIONS.ADD) {

  switch(op) {

    case config.OPERATIONS.ADD:
      return n1 + n2;

    case config.OPERATIONS.SUB:
      return n1 - n2;

    case config.OPERATIONS.MUL:
      return n1 * n2;

    case config.OPERATIONS.DIV:
      return n2 !== 0 ? n1 / n2 : 'Error: Division by zero';

    default:
      return n1 + n2;

  }

}

async function startBasicWorker() {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    

    await channel.assertExchange(config.TASK_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    await channel.assertQueue(config.TASK_QUEUE, { durable: true });
    await channel.bindQueue(config.TASK_QUEUE, config.TASK_EXCHANGE, 'task');
    

    await channel.assertExchange(config.RESULT_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    

    channel.prefetch(1);
    
    console.log('Worker connecté à RabbitMQ');
    console.log('En attente de tâches...');

    
    channel.consume(config.TASK_QUEUE, async (msg) => {

      if (msg !== null) {

        try {

          const content = JSON.parse(msg.content.toString());
          console.log(`[x] Reçu: ${JSON.stringify(content)}`);
          
          const processingTime = getRandomDelay();
          console.log(`[x] Traitement pendant ${processingTime}ms...`);
          await sleep(processingTime);
          

          const result = performOperation(content.n1, content.n2);
          

          const resultMessage = {
            n1: content.n1,
            n2: content.n2,
            op: config.OPERATIONS.ADD,
            result: result
          };
          

          channel.publish(
            config.RESULT_EXCHANGE,
            'result',
            Buffer.from(JSON.stringify(resultMessage)),
            { persistent: true }
          );

          
          console.log(`[x] Résultat envoyé: ${JSON.stringify(resultMessage)}`);
          channel.ack(msg);

        } catch (error) {
          console.error('Erreur de traitement:', error);
          channel.nack(msg);
        }

      }

    });

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}

async function startSpecializedWorker(operation) {

  if (!Object.values(config.OPERATIONS).slice(0, 4).includes(operation)) {
    console.error('Opération non reconnue. Utilisez: add, sub, mul ou div');
    process.exit(1);
  }

  
  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    

    let queueName;
    switch(operation) {

      case config.OPERATIONS.ADD: 
        queueName = config.ADD_QUEUE; break;

      case config.OPERATIONS.SUB: 
        queueName = config.SUB_QUEUE; break;

      case config.OPERATIONS.MUL: 
        queueName = config.MUL_QUEUE; break;

      case config.OPERATIONS.DIV: 
        queueName = config.DIV_QUEUE; break;

    }
    


    await channel.assertExchange(config.OPERATION_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    await channel.assertExchange(config.OPERATIONS.ALL, config.EXCHANGE_TYPES.FANOUT, { durable: true });
    await channel.assertExchange(config.RESULT_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    

    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, config.OPERATION_EXCHANGE, operation);
    await channel.bindQueue(queueName, config.OPERATIONS.ALL, '');

    
    channel.prefetch(1);
    
    console.log(`Worker pour l'opération ${operation} connecté à RabbitMQ`);
    console.log(`En attente de tâches sur ${queueName}...`);

    
    channel.consume(queueName, async (msg) => {

      if (msg !== null) {

        try {


          const content = JSON.parse(msg.content.toString());
          console.log(`[x] Reçu: ${JSON.stringify(content)}`);
          
          const requestedOp = content.op;
          const isAllOperation = requestedOp === config.OPERATIONS.ALL;
          
          if (!isAllOperation && requestedOp !== operation) {

            console.log(`[!] Opération ${requestedOp} reçue par un worker ${operation}, ignorée`);
            channel.ack(msg);
            return;

          }
         
          
          const processingTime = getRandomDelay();
          console.log(`[x] Traitement pendant ${processingTime}ms...`);
          await sleep(processingTime);
          

          const result = performOperation(content.n1, content.n2, operation);
          

          const resultMessage = {
            n1: content.n1,
            n2: content.n2,
            op: operation,
            result: result,
            allOperation: isAllOperation
          };
          

          const routingKey = isAllOperation ? 'all-result' : 'result';
          

          channel.publish(
            config.RESULT_EXCHANGE,
            routingKey,
            Buffer.from(JSON.stringify(resultMessage)),
            { persistent: true }
          );
          
          
          console.log(`[x] Résultat envoyé: ${JSON.stringify(resultMessage)}`);
          
          channel.ack(msg);

        } catch (error) {
          console.error('Erreur de traitement:', error);
          channel.nack(msg);
        }

      }

    });

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}


const args = process.argv.slice(2);
const operation = args[0];


if (!operation) {

  startBasicWorker();

} else if (operation === config.OPERATIONS.ALL) {

  console.log('AVERTISSEMENT: Il n\'y a plus de worker spécifique pour l\'opération "all".');
  console.log('Les opérations "all" sont maintenant envoyées à tous les workers spécialisés.');
  console.log('Veuillez démarrer des workers spécifiques (add, sub, mul, div) à la place.');

} else {

  startSpecializedWorker(operation);

}