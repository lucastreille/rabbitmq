const RABBITMQ_URL = 'amqp://user:password@infoexpertise.hopto.org:5676';

const TASK_QUEUE = 'task_queue';
const RESULT_QUEUE = 'result_queue';

const ADD_QUEUE = 'add_queue';
const SUB_QUEUE = 'sub_queue';
const MUL_QUEUE = 'mul_queue';
const DIV_QUEUE = 'div_queue';

const OPERATIONS = {
  ADD: 'add',
  SUB: 'sub',
  MUL: 'mul',
  DIV: 'div',
  ALL: 'all'
};

const REQUEST_INTERVAL = 5000;

const MIN_PROCESSING_TIME = 5000;
const MAX_PROCESSING_TIME = 15000;


const TASK_EXCHANGE = 'task_exchange';
const OPERATION_EXCHANGE = 'op_exchange';
const RESULT_EXCHANGE = 'result_exchange';


const EXCHANGE_TYPES = {
  DIRECT: 'direct',
  FANOUT: 'fanout'
};


module.exports = {
  RABBITMQ_URL,
  TASK_QUEUE,
  RESULT_QUEUE,
  ADD_QUEUE,
  SUB_QUEUE,
  MUL_QUEUE,
  DIV_QUEUE,
  OPERATIONS,
  REQUEST_INTERVAL,
  MIN_PROCESSING_TIME,
  MAX_PROCESSING_TIME,
  TASK_EXCHANGE,
  OPERATION_EXCHANGE,
  RESULT_EXCHANGE,
  EXCHANGE_TYPES
};