const { Kafka } = require('kafkajs');
const fs = require('fs');
const csvParser = require('csv-parser');
const { workerData, parentPort } = require('worker_threads');

const { start, end, supplyChainFile, topic, bootstrapServer } = workerData;

const kafka = new Kafka({
  clientId: `worker-${start}-${end}`,
  brokers: [bootstrapServer],
  retry: {
    initialRetryTime: 3000,
    retries: 10,
  },
});

const producer = kafka.producer();

const sendToKafka = async (message) => {
  try {
    await producer.send({
      topic: topic,
      messages: [{ value: JSON.stringify(message) }],
    });
  } catch (error) {
    console.error(`Error sending message:`, error);
  }
};

const processCSV = async () => {
  await producer.connect();

  let messageCount = 0;
  let rowNumber = 0;

  fs.createReadStream(supplyChainFile)
    .pipe(csvParser())
    .on('data', async (row) => {
      rowNumber++;
      if (rowNumber >= start && rowNumber <= end) {
        // Transform row to match the schema of your dataset
        const transformedRow = {
          Project_Code: row['cf1:Project_Code'],
          PQ: row['cf1:PQ#'],
          PO_SO: row['cf1:PO_SO#'],
          Country: row['cf1:Country'],
          Managed_By: row['cf1:Managed_By'],
          Fulfill_Via: row['cf1:Fulfill_Via'],
          Vendor_INCO_Term: row['cf1:Vendor_INCO_Term'],
          Shipment_Mode: row['cf1:Shipment_Mode'],
          PQ_First_Sent_to_Client_Date: row['cf1:PQ_First_Sent_to_Client_Date'],
          PO_Sent_to_Vendor_Date: row['cf1:PO_Sent_to_Vendor_Date'],
          Scheduled_Delivery_Date: row['cf1:Scheduled_Delivery_Date'],
          Delivered_to_Client_Date: row['cf1:Delivered_to_Client_Date'],
          Delivery_Recorded_Date: row['cf1:Delivery_Recorded_Date'],
          Product_Group: row['cf1:Product_Group'],
          Sub_Classification: row['cf1:Sub_Classification'],
          Vendor: row['cf1:Vendor'],
          Item_Description: row['cf1:Item_Description'],
          Molecule_Brand: row['cf1:Molecule_Brand'],
          Dosage: row['cf1:Dosage'],
          Dosage_Form: row['cf1:Dosage_Form'],
          Unit_of_Measure_Per_Pack: row['cf1:Unit_of_Measure_Per_Pack'],
          Line_Item_Quantity: parseFloat(row['cf1:Line_Item_Quantity']),
          Line_Item_Value: parseFloat(row['cf1:Line_Item_Value']),
          Pack_Price: parseFloat(row['cf1:Pack_Price']),
          Unit_Price: parseFloat(row['cf1:Unit_Price']),
          Manufacturing_Site: row['cf1:Manufacturing_Site'],
          First_Line_Designation: row['cf1:First_Line_Designation'],
          Weight_Kilograms: parseFloat(row['cf1:Weight_Kilograms']),
          Freight_Cost_USD: parseFloat(row['cf1:Freight_Cost_USD']),
          Line_Item_Insurance_USD: parseFloat(row['cf1:Line_Item_Insurance_USD']),
        };

        await sendToKafka(transformedRow);
        messageCount++;
      }
    })
    .on('end', async () => {
      console.log(`Worker processed ${messageCount} messages.`);
      await producer.disconnect();
      parentPort.postMessage(messageCount);
    })
    .on('error', (error) => {
      console.error('Error reading CSV:', error);
    });
};

processCSV().catch(console.error);
