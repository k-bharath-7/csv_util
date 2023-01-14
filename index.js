const CsvReadableStream = require("csv-reader");
const { createArrayCsvWriter, createObjectCsvWriter } = require("csv-writer");
const fs = require("fs");

const getFileName = (filePath) => filePath.split("/").pop();

/**
 * Read the csv file in the given file path and return as array of array / json
 *
 * @param {string} filePath - path of the csv file to read
 * @param {boolean} skipHeader - Boolean indicating whether to skip header or not,
 * 	by default it will not be skipped
 * @param {boolean} asObject - Boolean indicating whether to return the data as array
 * 	of array or json
 * @returns {Array<json>} - array containing data in each row as json object
 */
exports.csvFileReader = (
	filePath = "",
	skipHeader = false,
	asObject = false
) => {
	const csvData = [];
	return new Promise((resolve, reject) => {
		let inputstream = fs.createReadStream(filePath, "utf-8");
		inputstream
			.pipe(
				new CsvReadableStream({
					parseBooleans: true,
					parseNumbers: true,
					skipHeader,
					asObject,
					trim: true,
				})
			)
			.on("data", (row) => {
				csvData.push(row);
			})
			.on("error", (err) => {
				console.log(`***** Error: ${err.message} from csvFileReader.js ******`);
				reject(err);
			})
			.on("end", () => {
				console.log(
					` ====> CSV File - ${getFileName(filePath)} is read with ${
						csvData.length
					} rows \n`
				);
				resolve(csvData);
			});
	});
};

/**
 * Receives data for csv in  and write to the given file path with file name
 *
 * @param {string} filePath -
 * @param {Array<Array<T>>} csvData
 * @param {boolean} overwriteFile
 */
exports.createCSVWithArr = async (
	filePath,
	csvData = [[]],
	overwriteFile = false
) => {
	console.log("\n\nWriting records");
	//Creating csv with array doesnt need the headers
	if (!overwriteFile && fs.existsSync(filePath))
		throw new Error(
			`\nxxxxxxxx Create CSV error: ${filePath} already exists. Do you want to overwrite xxxxxxxx`
		);
	const arrCsvWriter = createArrayCsvWriter({
		path: filePath,
		encoding: "utf-8",
	});
	await arrCsvWriter.writeRecords(csvData);

	console.log(
		`\n+++++++++ CSV file (${getFileName(
			filePath
		)}) successfully created with ${csvData.length} rows +++++++++`
	);
};

/**
 * Receives data for csv as array of json object and write the data with given file name
 * 	and file path
 *
 * @param {string} filePath
 * @param {json} header
 * @param {} csvData
 * @param {boolean} overwriteFile
 */
exports.createCSVWithObj = async (
	filePath,
	header,
	csvData = [[]],
	overwriteFile = false
) => {
	if (!overwriteFile && fs.existsSync(filePath))
		throw new Error(
			`\nxxxxxxxx Create CSV error: ${filePath} already exists. Do you want to overwrite xxxxxxxx`
		);

	const objCsvWriter = createObjectCsvWriter({
		path: filePath,
		encoding: "utf-8",
		header,
		alwaysQuote: false,
	});
	await objCsvWriter.writeRecords(csvData);

	console.log(
		`\n+++++++++ CSV file (${getFileName(
			filePath
		)}) successfully created with ${csvData.length} rows +++++++++`
	);
};
