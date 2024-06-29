const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();
  console.log("Producer connecting ...");

  await producer.connect();
  console.log("Producer connected successfully");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async function (line) {
    const [riderName, location] = line.split(" ");
    await producer.send({
      topic: "rider-updates",
      messages: [
        {
          partition: location.toLowerCase() === "north" ? 0 : 1,
          key: "location-update",
          value: JSON.stringify({ name: riderName, location }),
        },
      ],
    });
  }).on("close", async () => {
    await producer.disconnect();
  });

  //   console.log("Sending Message ..");
  //   await producer.send({
  //     topic: "rider-updates",
  //     messages: [
  //       {
  //         partition: 0,
  //         key: "location-update",
  //         value: JSON.stringify({ name: "Tony Stark", loc: "SOUTH" }),
  //       },
  //     ],
  //   });

  //   console.log("Disconnecting producer ...");
  //   await producer.disconnect();
}

init();
