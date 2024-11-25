#!/usr/bin/env -S npx tsx

import { set } from 'lodash-es';
import { pino } from 'pino';
import { JsonWriteOptions, Message } from '@bufbuild/protobuf';
import { Protobuf } from '@meshtastic/js';
import { TextDecoder } from 'node:util';
import mqtt from 'mqtt';

const {
  LOG_LEVEL = 'warn',
  MQTT_HOST = 'raspberrypi:1883',
  ROOT_TOPIC = 'msh/PL',
} = process.env;

const logger = pino({ level: LOG_LEVEL });
const subTopic = `${ROOT_TOPIC}/2/e/#`;
const pubTopic = (sourceTopic: string) =>
  sourceTopic.replace(`${ROOT_TOPIC}/2/e`, `${ROOT_TOPIC}/2/decoder`);

const payloadDecoders: Record<number, Message | TextDecoder | null> = {
  [Protobuf.Portnums.PortNum.UNKNOWN_APP]: null,
  [Protobuf.Portnums.PortNum.TEXT_MESSAGE_APP]: new TextDecoder(),
  [Protobuf.Portnums.PortNum.REMOTE_HARDWARE_APP]:
    new Protobuf.RemoteHardware.HardwareMessage(),
  [Protobuf.Portnums.PortNum.POSITION_APP]: new Protobuf.Mesh.Position(),
  [Protobuf.Portnums.PortNum.NODEINFO_APP]: new Protobuf.Mesh.User(),
  [Protobuf.Portnums.PortNum.ROUTING_APP]: new Protobuf.Mesh.Routing(),
  [Protobuf.Portnums.PortNum.ADMIN_APP]: new Protobuf.Admin.AdminMessage(),
  [Protobuf.Portnums.PortNum.TEXT_MESSAGE_COMPRESSED_APP]: null,
  [Protobuf.Portnums.PortNum.WAYPOINT_APP]: new Protobuf.Mesh.Waypoint(),
  [Protobuf.Portnums.PortNum.AUDIO_APP]: null,
  [Protobuf.Portnums.PortNum.DETECTION_SENSOR_APP]: new TextDecoder(),
  [Protobuf.Portnums.PortNum.REPLY_APP]: new TextDecoder('ascii'),
  [Protobuf.Portnums.PortNum.IP_TUNNEL_APP]: null,
  [Protobuf.Portnums.PortNum.SERIAL_APP]: null,
  [Protobuf.Portnums.PortNum.STORE_FORWARD_APP]:
    new Protobuf.StoreForward.StoreAndForward(),
  [Protobuf.Portnums.PortNum.RANGE_TEST_APP]: new TextDecoder('ascii'),
  [Protobuf.Portnums.PortNum.TELEMETRY_APP]: new Protobuf.Telemetry.Telemetry(),
  [Protobuf.Portnums.PortNum.ZPS_APP]: null,
  [Protobuf.Portnums.PortNum.SIMULATOR_APP]: null,
  [Protobuf.Portnums.PortNum.TRACEROUTE_APP]: null,
  [Protobuf.Portnums.PortNum.NEIGHBORINFO_APP]:
    new Protobuf.Mesh.NeighborInfo(),
  [Protobuf.Portnums.PortNum.PRIVATE_APP]: null,
  [Protobuf.Portnums.PortNum.ATAK_FORWARDER]: null,
};

function decodeMessage(message: Buffer) {
  if (!Buffer.isBuffer(message)) {
    logger.error('Message is not a buffer. Exiting without emitting msg.');
    return null;
  }

  const serviceEnvelope = Protobuf.Mqtt.ServiceEnvelope.fromBinary(message);
  if (!serviceEnvelope.packet) {
    logger.debug('No packet in ServiceEnvelope. Exiting without emitting msg.');
    return null;
  }

  const jsonWriteOptions: Partial<JsonWriteOptions> = {
    emitDefaultValues: true,
    enumAsInteger: false,
  };

  logger.debug('Serializing ServiceEnvelope to JSON for output.');
  const result = serviceEnvelope.toJson(jsonWriteOptions);

  switch (serviceEnvelope.packet.payloadVariant.case) {
    case 'encrypted':
      logger.debug(
        'Payload was encrypted. Returning serialized ServiceEnvelope.'
      );
      break;
    case 'decoded': {
      try {
        const portNum = serviceEnvelope.packet.payloadVariant.value.portnum;

        if (!payloadDecoders[portNum]) {
          logger.debug(`No decoder set for portnum ${portNum}.`);
          break;
        }

        const { payload } = serviceEnvelope.packet.payloadVariant.value;
        const decoder = payloadDecoders[portNum];

        if (decoder instanceof TextDecoder) {
          logger.debug('TextDecoder detected. Decoding payload.');
          set(
            result as object,
            'packet.decoded.payload',
            decoder.decode(payload)
          );
        } else {
          logger.debug(
            'Decoder was not null and not a TextDecoder. Assuming Protobuf decoder and decoding payload.'
          );
          set(
            result as object,
            'packet.decoded.payload',
            decoder.fromBinary(payload).toJson(jsonWriteOptions)
          );
        }

        logger.debug({ msg: 'Decoded payload to JSON', result });
      } catch (error) {
        logger.error(`could not decode payload: ${error}`);
      }

      break;
    }
  }

  logger.debug('Outputting payload from decode node.');
  return result;
}

const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}`, {
  connectTimeout: 1000,
  reconnectPeriod: 5000,
});

mqttClient.on('connect', () => {
  mqttClient.subscribe(subTopic, (error) => {
    if (error) {
      console.error(`Error while subscribing to ${subTopic}: ${error}`);
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const decodedMessage = decodeMessage(message);
    mqttClient.publish(pubTopic(topic), JSON.stringify(decodedMessage));
  } catch (error) {
    logger.error(`Could not publish message: ${error}`);
  }
});

const gracefulExit = () => {
  logger.info('Exiting...');
  mqttClient.end();
  process.exit(1);
};

process.on('SIGTERM', gracefulExit);
process.on('SIGINT', gracefulExit);
