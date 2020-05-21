// based on Relay's Connection spec at
// https://facebook.github.io/relay/graphql/connections.htm#sec-Pagination-algorithm

/**
 * Slices the nodes list according to the `before` and `after` graphql query params.
 * @param {Object} allNodesAccessor an accessor to the nodes. Will depend on the concrete implementor.
 * @param {Object} graphqlParams must contain `before` and `after` query params.
 * @param {Object} operatorFunctions must contain removeNodesBeforeAndIncluding and removeNodesAfterAndIncluding functions.
 * @param {Object} orderArgs must contain orderColumn and ascOrDesc. Include only if the implementor requires these params.
 */
const applyCursorsToNodes = (
  allNodesAccessor,
  { before, after }, {
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
  }, {
    idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
  },
) => {
  let nodesAccessor = allNodesAccessor;
  if (after) {
    nodesAccessor = removeNodesBeforeAndIncluding(nodesAccessor, after, {
      idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
    });
  }
  if (before) {
    nodesAccessor = removeNodesAfterAndIncluding(nodesAccessor, before, {
      idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
    });
  }
  return nodesAccessor;
};

/**
 * Slices a node list according to `before`, `after`, `first` and `last` graphql query params.
 * @param {Object} allNodesAccessor an accessor to the nodes. Will depend on the concrete implementor.
 * @param {Object} operatorFunctions must contain `getNodesLength`, `removeNodesFromEnd`, `removeNodesFromBeginning`,`removeNodesBeforeAndIncluding` and `removeNodesAfterAndIncluding` functions.
 * @param {Object} graphqlParams must contain `first`, `last`, `before` and `after` query params.
 * @param {Object} orderArgs must contain orderColumn and ascOrDesc. Include only if the implementor requires these params.
 */
const nodesToReturn = async (
  allNodesAccessor,
  {
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
    hasLengthGreaterThan,
    removeNodesFromEnd,
    removeNodesFromBeginning,
    orderNodesBy,
  },
  {
    before, after, first, last,
  }, {
    idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
  },
) => {
  const orderedNodesAccessor = orderNodesBy(allNodesAccessor, {
    idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
  });
  const nodesAccessor = applyCursorsToNodes(
    orderedNodesAccessor,
    { before, after },
    {
      removeNodesBeforeAndIncluding,
      removeNodesAfterAndIncluding,
    }, {
      idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
    },
  );
  let hasNextPage = !!before;
  let hasPreviousPage = !!after;
  let nodes = [];
  if (first) {
    if (first < 0) throw new Error('`first` argument must not be less than 0');
    nodes = await removeNodesFromEnd(nodesAccessor, first + 1, { idColumn, orderColumn, ascOrDesc, formatColumnOptions });
    if (nodes.length > first) {
      hasNextPage = true;
      nodes = nodes.slice(0, first);
    }
  }
  if (last) {
    if (last < 0) throw new Error('`last` argument must not be less than 0');
    nodes = await removeNodesFromBeginning(nodesAccessor, last + 1, { idColumn, orderColumn, ascOrDesc, formatColumnOptions });
    if (nodes.length > last) {
      hasPreviousPage = true;
      nodes = nodes.slice(1);
    }
  }
  return { nodes, hasNextPage, hasPreviousPage };
};

/**
 * Returns a function that must be called to generate a Relay's Connection based page.
 * @param {Object} operatorFunctions must contain `getNodesLength`, `removeNodesFromEnd`, `removeNodesFromBeginning`,`removeNodesBeforeAndIncluding` and `removeNodesAfterAndIncluding` functions.
 */
const apolloCursorPaginationBuilder = ({
  removeNodesBeforeAndIncluding,
  removeNodesAfterAndIncluding,
  getNodesLength,
  hasLengthGreaterThan,
  removeNodesFromEnd,
  removeNodesFromBeginning,
  convertNodesToEdges,
  orderNodesBy,
}) => async (
  allNodesAccessor,
  {
    before, after, first, last, orderBy = 'id', orderDirection = 'asc',
  },
  opts = {},
) => {
  const {
    idColumn, isAggregateFn, formatColumnOptions, skipTotalCount = false, modifyNodeFn, getTotal = true
  } = opts;
  let {
    orderColumn, ascOrDesc,
  } = opts;
  if (orderColumn) {
    console.warn('"orderColumn" and "ascOrDesc" are being deprecated in favor of "orderBy" and "orderDirection" respectively');
  } else {
    orderColumn = orderBy;
    ascOrDesc = orderDirection;
  }

  let { nodes, hasPreviousPage, hasNextPage } = await nodesToReturn(
    allNodesAccessor,
    {
      removeNodesBeforeAndIncluding,
      removeNodesAfterAndIncluding,
      getNodesLength,
      hasLengthGreaterThan,
      removeNodesFromEnd,
      removeNodesFromBeginning,
      orderNodesBy,
    }, {
      before, after, first, last,
    }, {
      idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
    },
  );

  let totalCount = 0;
  if (getTotal) {
    totalCount = !skipTotalCount && await getNodesLength(allNodesAccessor, {
      getNodesLength,
    });
  }

  if (modifyNodeFn) {
    nodes = nodes.map(node => modifyNodeFn(node));
  }
  let edges = convertNodesToEdges(nodes, {
    before, after, first, last,
  }, {
    idColumn, orderColumn, ascOrDesc, isAggregateFn, formatColumnOptions,
  });


  const startCursor = edges[0] && edges[0].cursor;
  const endCursor = edges[edges.length - 1] && edges[edges.length - 1].cursor;

  return {
    pageInfo: {
      hasPreviousPage,
      hasNextPage,
      startCursor,
      endCursor,
    },
    totalCount,
    edges,
  };
};

module.exports = apolloCursorPaginationBuilder;
