const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  //   const consumer = kafka.consumer({ groupId: "user-1" });
  const consumer = kafka.consumer({ groupId: group });

  await consumer.connect();

  console.log("Consume connected successfully ... ");
  await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `${group}:  [${topic}]: PART:${partition}:`,
        message.value.toString()
      );
    },
  });
}

init();
