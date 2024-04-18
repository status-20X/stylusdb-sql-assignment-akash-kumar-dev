const { parseDeleteQuery } = require("./queryParser");
const { readCSV, writeCSV } = require("./csvReader");

async function executeDELETEQuery(query) {
  const { table, whereClauses } = parseDeleteQuery(query);
  let data = await readCSV(`${table}.csv`);

  if (whereClauses.length > 0) {
    data = data.filter(row => !whereClauses.every(clause => evaluateCondition(row, clause)));
  }

  await writeCSV(`${table}.csv`, data);
}

function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  // Get the actual value from the row
  const actualValue = row[field];

  // Convert the actual value and the value in the condition to appropriate types for comparison
  const convertedActualValue = isNaN(actualValue)
    ? actualValue
    : parseFloat(actualValue);
  const convertedValue = isNaN(value) ? value : parseFloat(value);
  if (operator === "LIKE") {
    // Transform SQL LIKE pattern to JavaScript RegExp pattern
    const regexPattern =
      "^" + value.replace(/%/g, ".*").replace(/_/g, ".") + "$";
    const regex = new RegExp(regexPattern, "i"); // 'i' for case-insensitive matching
    return regex.test(row[field]);
  }
  // Perform the comparison based on the operator
  switch (operator) {
    case "=":
      return convertedActualValue === convertedValue;
    case "!=":
      return convertedActualValue !== convertedValue;
    case ">":
      return convertedActualValue > convertedValue;
    case "<":
      return convertedActualValue < convertedValue;
    case ">=":
      return convertedActualValue >= convertedValue;
    case "<=":
      return convertedActualValue <= convertedValue;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

module.exports = executeDELETEQuery;