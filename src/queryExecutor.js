const { readCSV, writeCSV } = require('./csvReader');
const { parseInsertQuery } = require('./queryParser');
async function executeINSERTQuery(query) {
    const { table, columns, values, returningColumns } = parseInsertQuery(query);
    const data = await readCSV(`${table}.csv`);

    // Check if 'id' column is included in the query and in CSV headers
    let newId = null;
    if (!columns.includes('id') && data.length > 0 && 'id' in data[0]) {
        // 'id' column not included in the query, so we auto-generate an ID
        const existingIds = data.map(row => parseInt(row.id)).filter(id => !isNaN(id));
        const maxId = existingIds.length > 0 ? Math.max(...existingIds) : 0;
        newId = maxId + 1;
        columns.push('id');
        values.push(newId.toString()); // Add as a string
    }

    // Create a new row object matching the CSV structure
    const headers = data.length > 0 ? Object.keys(data[0]) : columns;
    const newRow = {};
    headers.forEach(header => {
        const columnIndex = columns.indexOf(header);
        if (columnIndex !== -1) {
            let value = values[columnIndex];
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length - 1);
            }
            newRow[header] = value;
        } else {
            newRow[header] = header === 'id' ? newId.toString() : '';
        }
    });

    // Add the new row to the data
    data.push(newRow);

    // Save the updated data back to the CSV file
    await writeCSV(`${table}.csv`, data);

    // Prepare the returning result if returningColumns are specified
    let returningResult = {};
    if (returningColumns.length > 0) {
        returningColumns.forEach(column => {
            returningResult[column] = newRow[column];
        });
    }

    return {
        message: "Row inserted successfully.",
        insertedId: newId,
        returning: returningResult
    };
}

module.exports = executeINSERTQuery;