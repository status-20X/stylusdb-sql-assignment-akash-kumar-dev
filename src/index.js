const { parseQuery } = require("./queryParser");
const { readCSV } = require("./csvReader");
const {
  performInnerJoin,
  performLeftJoin,
  performRightJoin,
} = require("./joins");
async function executeSELECTQuery(query) {
  try {
    const {
      fields,
      table,
      whereClauses,
      joinType,
      joinTable,
      joinCondition,
      groupByFields,
      hasAggregateWithoutGroupBy,
      isApproximateCount,
      orderByFields,
      limit,
      isDistinct,
      distinctFields,
      isCountDistinct,
    } = parseQuery(query);

    if (
      isApproximateCount &&
      fields.length === 1 &&
      fields[0] === "COUNT(*)" &&
      whereClauses.length === 0
    ) {
      let hll = await readCSVForHLL(`${table}.csv`);
      return [{ "APPROXIMATE_COUNT(*)": hll.estimate() }];
    }

    let data = await readCSV(`${table}.csv`);

    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
      const joinData = await readCSV(`${joinTable}.csv`);
      switch (joinType.toUpperCase()) {
        case "INNER":
          data = performInnerJoin(data, joinData, joinCondition, fields, table);
          break;
        case "LEFT":
          data = performLeftJoin(data, joinData, joinCondition, fields, table);
          break;
        case "RIGHT":
          data = performRightJoin(data, joinData, joinCondition, fields, table);
          break;
        default:
          throw new Error(`Unsupported JOIN type: ${joinType}`);
      }
    }
    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    let filteredData =
      whereClauses.length > 0
        ? data.filter((row) =>
            whereClauses.every((clause) => evaluateCondition(row, clause))
          )
        : data;

    let groupResults = filteredData;
    if (hasAggregateWithoutGroupBy) {
      // Special handling for queries like 'SELECT COUNT(*) FROM table'
      const result = {};

      fields.forEach((field) => {
        const match = /(\w+)\((\*|\w+)\)/.exec(field);
        if (match) {
          const [, aggFunc, aggField] = match;
          switch (aggFunc.toUpperCase()) {
            case "COUNT":
              result[field] = filteredData.length;
              break;
            case "SUM":
              result[field] = filteredData.reduce(
                (acc, row) => acc + parseFloat(row[aggField]),
                0
              );
              break;
            case "AVG":
              result[field] =
                filteredData.reduce(
                  (acc, row) => acc + parseFloat(row[aggField]),
                  0
                ) / filteredData.length;
              break;
            case "MIN":
              result[field] = Math.min(
                ...filteredData.map((row) => parseFloat(row[aggField]))
              );
              break;
            case "MAX":
              result[field] = Math.max(
                ...filteredData.map((row) => parseFloat(row[aggField]))
              );
              break;
            // Additional aggregate functions can be handled here
          }
        }
      });

      return [result];
      // Add more cases here if needed for other aggregates
    } else if (groupByFields) {
      groupResults = applyGroupBy(filteredData, groupByFields, fields);

      // Order them by the specified fields
      let orderedResults = groupResults;
      if (orderByFields) {
        orderedResults = groupResults.sort((a, b) => {
          for (let { fieldName, order } of orderByFields) {
            if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
            if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
          }
          return 0;
        });
      }
      if (limit !== null) {
        groupResults = groupResults.slice(0, limit);
      }
      return groupResults;
    } else {
      // Order them by the specified fields
      let orderedResults = groupResults;
      if (orderByFields) {
        orderedResults = groupResults.sort((a, b) => {
          for (let { fieldName, order } of orderByFields) {
            if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
            if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
          }
          return 0;
        });
      }

      // Distinct inside count - example "SELECT COUNT (DISTINCT student.name) FROM student"
      if (isCountDistinct) {
        if (isApproximateCount) {
          var h = hll({ bitSampleSize: 12, digestSize: 128 });
          orderedResults.forEach((row) =>
            h.insert(distinctFields.map((field) => row[field]).join("|"))
          );
          return [{ [`APPROXIMATE_${fields[0]}`]: h.estimate() }];
        } else {
          let distinctResults = [
            ...new Map(
              orderedResults.map((item) => [
                distinctFields.map((field) => item[field]).join("|"),
                item,
              ])
            ).values(),
          ];
          return [{ [fields[0]]: distinctResults.length }];
        }
      }

      // Select the specified fields
      let finalResults = orderedResults.map((row) => {
        const selectedRow = {};
        fields.forEach((field) => {
          // Assuming 'field' is just the column name without table prefix
          selectedRow[field] = row[field];
        });
        return selectedRow;
      });

      // Remove duplicates if specified
      let distinctResults = finalResults;
      if (isDistinct) {
        distinctResults = [
          ...new Map(
            finalResults.map((item) => [
              fields.map((field) => item[field]).join("|"),
              item,
            ])
          ).values(),
        ];
      }

      let limitResults = distinctResults;
      if (limit !== null) {
        limitResults = distinctResults.slice(0, limit);
      }

      return limitResults;
    }
  } catch (error) {
    throw new Error(`Error executing query: ${error.message}`);
  }
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

function applyGroupBy(data, groupByFields, aggregateFunctions) {
  const groupResults = {};

  data.forEach((row) => {
    // Generate a key for the group
    const groupKey = groupByFields.map((field) => row[field]).join("-");

    // Initialize group in results if it doesn't exist
    if (!groupResults[groupKey]) {
      groupResults[groupKey] = { count: 0, sums: {}, mins: {}, maxes: {} };
      groupByFields.forEach(
        (field) => (groupResults[groupKey][field] = row[field])
      );
    }

    // Aggregate calculations
    groupResults[groupKey].count += 1;
    aggregateFunctions.forEach((func) => {
      const match = /(\w+)\((\w+)\)/.exec(func);
      if (match) {
        const [, aggFunc, aggField] = match;
        const value = parseFloat(row[aggField]);

        switch (aggFunc.toUpperCase()) {
          case "SUM":
            groupResults[groupKey].sums[aggField] =
              (groupResults[groupKey].sums[aggField] || 0) + value;
            break;
          case "MIN":
            groupResults[groupKey].mins[aggField] = Math.min(
              groupResults[groupKey].mins[aggField] || value,
              value
            );
            break;
          case "MAX":
            groupResults[groupKey].maxes[aggField] = Math.max(
              groupResults[groupKey].maxes[aggField] || value,
              value
            );
            break;
          // Additional aggregate functions can be added here
        }
      }
    });
  });

  // Convert grouped results into an array format
  return Object.values(groupResults).map((group) => {
    // Construct the final grouped object based on required fields
    const finalGroup = {};
    groupByFields.forEach((field) => (finalGroup[field] = group[field]));
    aggregateFunctions.forEach((func) => {
      const match = /(\w+)\((\*|\w+)\)/.exec(func);
      if (match) {
        const [, aggFunc, aggField] = match;
        switch (aggFunc.toUpperCase()) {
          case "SUM":
            finalGroup[func] = group.sums[aggField];
            break;
          case "MIN":
            finalGroup[func] = group.mins[aggField];
            break;
          case "MAX":
            finalGroup[func] = group.maxes[aggField];
            break;
          case "COUNT":
            finalGroup[func] = group.count;
            break;
          // Additional aggregate functions can be handled here
        }
      }
    });

    return finalGroup;
  });
}

module.exports = executeSELECTQuery;