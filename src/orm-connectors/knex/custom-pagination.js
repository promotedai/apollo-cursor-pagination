const base64 = require('base-64');
const apolloCursorPaginationBuilder = require('../../builder');
const { applyFilters } = require('knex-graphql-filters');

// We encode strings in the token using encodeURIComponent.  If the following tokens were
// in the values, they'd be encoded.
const SEPARATION_TOKEN = '/';

const encode = str => base64.encode(str);
const decode = str => base64.decode(str);

// TODO - this can probably be removed to make the code easier to read.
const operateOverScalarOrArray = (initialValue, scalarOrArray, operation, operateResult) => {
  let result = initialValue;
  const isArray = Array.isArray(scalarOrArray);
  if (isArray) {
    scalarOrArray.forEach((scalar, index) => {
      result = operation(scalar, index, result);
    });
  } else {
    result = operation(scalarOrArray, null, result);
  }
  if (operateResult) {
    result = operateResult(result, isArray);
  }

  return result;
};

const getDataFromCursor = (cursor) => {
  const decodedCursor = decode(cursor);
  return decodedCursor.split(SEPARATION_TOKEN).map(v => JSON.parse(decodeURIComponent(v)));
};

const getNodeValue = (node, fieldPath) => {
  const fieldParts = fieldPath.split(".");
  const nodeValue = fieldParts.reduce((aggregator, field) => {
    return aggregator && aggregator[field]
  }, node)
  return nodeValue;
}

// Receives a list of nodes and returns it in edge form:
// {
//   cursor
//   node
// }
const convertNodesToEdges = (nodes, _, {
  idColumn,
  orderColumn,
}) => nodes.map((node) => {
  orderColumn = combineOrderColumn(orderColumn, idColumn);
  const cursor = encode(operateOverScalarOrArray('', orderColumn, (orderBy, index, prev) => {
    const nodeValue = getNodeValue(node, orderBy);
    const result = `${prev}${index ? SEPARATION_TOKEN : ''}${encodeURIComponent(JSON.stringify(nodeValue))}`;
    return result;
  }));

  return {
    cursor,
    node,
  };
});

const formatColumnIfAvailable = (column, formatColumnOptions, forWhereClause) => {
  if (formatColumnOptions && formatColumnOptions.columnFormatter) {
    return formatColumnOptions.columnFormatter(column, forWhereClause);
  }
  return column;
};

const asArray = (arr) => {
  if (Array.isArray(arr)) {
    return arr;
  } else {
    return [arr];
  }
}

const copyAsArray = (arr) => {
  if (Array.isArray(arr)) {
    return [...arr];
  } else {
    return [arr];
  }
}

// Returns true if we find findColumn in orderColumn.
const idContains = (orderColumns, findSingleColumn) => {
  return orderColumns.includes(findSingleColumn);
}

const combineOrderColumn = (orderColumn, idColumn) => {
  var results = copyAsArray(orderColumn);
  asArray(idColumn).forEach((idCol) => {
    if (!idContains(orderColumn, idCol)) {
      results.push(idCol);
    }
  });
  return results;
}

// Do a combined version since we de-dupe for both results.
// This returns a tuple array with both orderColumn and ascOrDesc.
const combineOrderColumnAndAscOrDesc = (orderColumn, idColumn, ascOrDesc) => {
  var resultOrderColumns = copyAsArray(orderColumn);
  var resultAscOrDesc = copyAsArray(ascOrDesc);
  const lastAscOrDesc = resultAscOrDesc[resultAscOrDesc.length - 1];
  asArray(idColumn).forEach((idCol) => {
    if (!idContains(resultOrderColumns, idCol)) {
      resultOrderColumns.push(idCol);
      resultAscOrDesc.push(lastAscOrDesc);
    }
  });

  if (resultOrderColumns.length != resultAscOrDesc.length) {
    console.warn(`order and ascOrDesc lengths do not match, resultOrderColumns=${resultOrderColumns} resultAscOrDesc=${resultAscOrDesc}`);
  }
  return [resultOrderColumns, resultAscOrDesc];
}


const buildRemoveNodesFromBeforeOrAfter = (beforeOrAfter) => {
  const getComparator = orderDirection => {
    if (beforeOrAfter === 'after') return orderDirection === 'asc' ? 'lt' : 'gt';
    return orderDirection === 'asc' ? 'gt' : 'lt';
  };
  return (nodesAccessor, cursorOfInitialNode, {
    idColumn = 'id',
    orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
  }) => {
    const cursorValues = getDataFromCursor(cursorOfInitialNode);

    const executeFilterQuery = query => {

      // The following doc talks about how the origin branch has an incorrect multiple column implementation.
      // https://docs.google.com/document/d/1G_7JCNgkg4avwZCojwdwSmJoFvvbr5_K4UJpRXlTq-k/edit#

      // Merge idColumn into a copy of orderColumn and ascOrDesc.
      [orderColumn, ascOrDesc] = combineOrderColumnAndAscOrDesc(orderColumn, idColumn, ascOrDesc);

      const filters = operateOverScalarOrArray({ OR: []}, orderColumn, (orderBy, index, prev) => {
        let orderDirection;
        let currValue;
        if (index !== null) {
          orderDirection = ascOrDesc[index].toLowerCase();
          currValue = cursorValues[index];
        } else {
          orderDirection = ascOrDesc.toLowerCase();
          currValue = cursorValues[0];
        }

        // TODO - handle having vs where.

        const conditions = {};
        // For each pass, we want the previous columns to match their value.
        // TODO - add description for why.
        for (var i = 0; i < index; i++) {
          const columnName = formatColumnIfAvailable(orderColumn[i], formatColumnOptions, true);
          conditions[columnName] = { is: cursorValues[i] };
        }
        // The condition for the current index should use the comparator.
        const columnName = formatColumnIfAvailable(orderBy, formatColumnOptions, true);

        // We treat NULL as 0.  If we're trying to reach values that are
        // not possible < NULL or > NOT NULL, skip that condition (since it's an OR).
        if (currValue === null || currValue === undefined) {
          if (orderDirection === 'desc') {
            // With our current sorting, We can't a value less than null.
            return prev;
          } else {
            // We consider "not null" to be greater than "null".
            conditions[columnName] = { not_null: true };
          }
        } else {
          const comparator = getComparator(orderDirection);
          conditions[columnName] = { [comparator]: currValue };
        }

        if (index == 0) {
          // We don't need wrapping parantheses.
          prev.OR.push(conditions);
        } else {
          prev.OR.push({
            AND: [conditions]
          });
        }
        return prev;
      });

      const areAnyColumnsAggregates =
        orderColumn.reduce((accumulator, currentColumn) => accumulator || isAggregateFn(currentColumn), false);
      const opts = {...formatColumnOptions};
      if (areAnyColumnsAggregates) {
        opts.having = true;
      }
      return applyFilters(query, filters, opts);
    }

    return executeFilterQuery(nodesAccessor.clone());
  };
};

const orderNodesBy = (nodesAccessor, { idColumn, orderColumn = 'id', ascOrDesc = 'asc', formatColumnOptions }) => {
  [orderColumn, ascOrDesc] = combineOrderColumnAndAscOrDesc(orderColumn, idColumn, ascOrDesc);

  const initialValue = nodesAccessor.clone();
  const result = operateOverScalarOrArray(initialValue, orderColumn, (orderBy, index, prev) => {
    if (index !== null) {
      return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnOptions), ascOrDesc[index]);
    }
    return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnOptions), ascOrDesc);
  });
  return result;
};

// Used when `after` is included in the query
// It must slice the result set from the element after the one with the given cursor until the end.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesBeforeAndIncluding(resultSet, 'B') should return [C, D]
const removeNodesBeforeAndIncluding = buildRemoveNodesFromBeforeOrAfter('before');

// Used when `first` is included in the query
// It must remove nodes from the result set starting from the end until it's of size `length`.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesFromEnd(resultSet, 3) should return [A, B, C]
const removeNodesFromEnd = (nodesAccessor, first) => nodesAccessor.clone().limit(first);

// Used when `before` is included in the query
// It must remove all nodes after and including the one with cursor `cursorOfInitialNode`
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesAfterAndIncluding(resultSet, 'C') should return [A, B]
const removeNodesAfterAndIncluding = buildRemoveNodesFromBeforeOrAfter('after');

// Used when `last` is included in the query
// It must remove nodes from the result set starting from the beginning until it's of size `length`.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesFromBeginning(resultSet, 3) should return [B, C, D]
const removeNodesFromBeginning = async (nodesAccessor, last, { idColumn, orderColumn, ascOrDesc, formatColumnOptions }) => {
  // Flip the sort ordering.
  const inverseAscOrDesc = operateOverScalarOrArray([], ascOrDesc,
    (orderDirection, index, prev) => prev.concat(orderDirection === 'asc' ? 'desc' : 'asc'));

  // TODO - I don't know how this works.
  //  We don't need the idColumn
  const subquery = orderNodesBy(nodesAccessor.clone().clearOrder(), { idColumn, orderColumn, ascOrDesc: inverseAscOrDesc, formatColumnOptions}).limit(last);

  const result = nodesAccessor.clone().from(subquery.as('last_subquery')).clearSelect().clearWhere();

  return result;
};


const getNodesLength = async (nodesAccessor) => {
  const counts = await nodesAccessor.clone().count('*');
  const result = counts.reduce((prev, curr) => {
    const currCount = curr.count || curr['count(*)'];
    if (!currCount) return prev;
    return parseInt(currCount, 10) + prev;
  }, 0);
  return result;
};

const hasLengthGreaterThan = async (nodesAccessor, amount) => {
  const result = await nodesAccessor.clone().limit(amount + 1);
  return result.length === amount + 1;
};

const paginate = apolloCursorPaginationBuilder(
  {
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
    getNodesLength,
    hasLengthGreaterThan,
    removeNodesFromEnd,
    removeNodesFromBeginning,
    convertNodesToEdges,
    orderNodesBy,
  },
);

module.exports = paginate;
module.exports.getDataFromCursor = getDataFromCursor;
module.exports.removeNodesBeforeAndIncluding = removeNodesBeforeAndIncluding;
module.exports.removeNodesFromEnd = removeNodesFromEnd;
module.exports.removeNodesAfterAndIncluding = removeNodesAfterAndIncluding;
module.exports.removeNodesFromBeginning = removeNodesFromBeginning;
module.exports.getNodesLength = getNodesLength;
module.exports.hasLengthGreaterThan = hasLengthGreaterThan;
module.exports.convertNodesToEdges = convertNodesToEdges;
