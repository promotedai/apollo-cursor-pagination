const base64 = require('base-64');
const knexCreator = require('knex');
const knexStringcase = require('knex-stringcase');
const converterFactory = require('knex-stringcase/converter-factory');
const { knexPaginator } = require('../src');
const { applyFilters } = require('knex-graphql-filters');

const cachedSnakecase = converterFactory('snakecase');

// We want to assert against the query strings.  The pagination code is structured to
// execute the queries.  To work around this, we'll wrap the knex query and capture
// strings and return dummy data.
class OutputQueryStringsKnex {
  constructor(query, queryStringsOutput) {
    this.query = query;
    this.queryStringsOutput = queryStringsOutput;
  }

  clone() {
    return new OutputQueryStringsKnex(this.query.clone(), this.queryStringsOutput);
  }

  orderBy(...args) {
    this.query.orderBy(...args);
    return this;
  }

  clearSelect(...args) {
    this.query.clearSelect(...args);
    return this;
  }

  clearWhere(...args) {
    this.query.clearWhere(...args);
    return this;
  }

  clearOrder(...args) {
    this.query.clearOrder(...args);
    return this;
  }

  as(...args) {
    this.query.as(...args);
    return this;
  }

  from(...args) {
    this.query.from(...args);
    return this;
  }

  andWhere(...args) {
    this.query.andWhere(...args);
    return this;
  }

  orWhere(...args) {
    this.query.orWhere(...args);
    return this;
  }

  having(...args) {
    this.query.having(...args);
    return this;
  }

  havingRaw(...args) {
    this.query.havingRaw(...args);
    return this;
  }

  andHaving(...args) {
    this.query.andHaving(...args);
    return this;
  }

  orHaving(...args) {
    this.query.orHaving(...args);
    return this;
  }

  orHavingRaw(...args) {
    this.query.orHavingRaw(...args);
    return this;
  }

  count(...args) {
    this.query.count(...args);
    return this;
  }

  limit(...args) {
    this.query.limit(...args);
    return this;
  }

  then(resolveFn, rejectFn) {
    this.queryStringsOutput.push(this.query.toString());
    resolveFn([]);
  }
}

function paginate(baseQuery, cursorInput, idName) {
  return knexPaginator(baseQuery, cursorInput, {
    idColumn: idName,
    isAggregateFn: (column) => {
      return column == "myMetric1" || column == "myMetric2" || column == "myMetric3";;
    },
    formatColumnFn: column => {
      if (column == "myMetric1") {
        return "sum(myMetric1)";
      } else if (column == "myMetric2") {
        return "sum(my_metric2)";
      } else if (column == "myMetric3") {
        return "custom_forth_column";
      }
      return cachedSnakecase(column);
    },
  });
}

function createTestKnex() {
  const knex = knexCreator(
    knexStringcase({
      // The mysql client causes weird backticks that breaks Presto.
      client: 'postgres',
    })
  );
  return knex("mytable").select("metric");
}

function wrapKnex(query, queryStringsOutput) {
  return new OutputQueryStringsKnex(query, queryStringsOutput);
};

const createTestCursorInput = () => ({
  first: 3,
  after: base64.encode("2/1"),
  orderBy: "firstId",
  orderDirection: "desc"
});

describe('no sort', () => {
  it('no sort', async () => {
    const queryStrings = [];
    const cursorInput = {};
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(1);
    expect(queryStrings[0]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });
});

describe('test where clause', () => {

  it('desc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 5,
      orderBy: "firstId",
      orderDirection: "desc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"first_id\" desc, \"backup_id\" desc limit 6");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('asc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 5,
      orderBy: "firstId",
      orderDirection: "asc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"first_id\" asc, \"backup_id\" asc limit 6");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort desc', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"backup_id\" < 1)) order by \"first_id\" desc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  describe('handling the id columns', () => {

    it('using string IDs', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 5,
        after: base64.encode("\"2\"/\"1\""),
        orderBy: "firstId",
        orderDirection: "desc"
      };

      await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < \'2\' or (\"first_id\" = \'2\' and \"backup_id\" < \'1\')) order by \"first_id\" desc, \"backup_id\" desc limit 6");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('multiple IDs', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 5,
        after: base64.encode("\"2\"/\"1\"/\"3\""),
        orderBy: "firstId",
        orderDirection: "desc"
      };

      await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, ['backupId1', 'backupId2']);
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < '2' or (\"first_id\" = '2' and \"backup_id1\" < '1') or (\"first_id\" = '2' and \"backup_id1\" = '1' and \"backup_id2\" < '3')) order by \"first_id\" desc, \"backup_id1\" desc, \"backup_id2\" desc limit 6")
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('de-dupe ID', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 5,
        after: base64.encode("\"2\""),
        orderBy: "firstId",
        orderDirection: "desc"
      };

      await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'firstId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < \'2\') order by \"first_id\" desc limit 6");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('multiple IDs - de-dupe one ID', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 5,
        after: base64.encode("1/\"2\""),
        orderBy: ["firstId", "secondId"],
        orderDirection: ["desc", "asc"],
      };

      await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'secondId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 1 or (\"first_id\" = 1 and \"second_id\" > '2')) order by \"first_id\" desc, \"second_id\" asc limit 6");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('multiple IDs - de-dupe all ID', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 5,
        after: base64.encode("1/\"2\"/\"3\""),
        orderBy: ["firstId", "secondId", "thirdId"],
        orderDirection: ["desc", "asc", "asc"],
      };

      await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, ["secondId", "thirdId", "firstId"]);
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 1 or (\"first_id\" = 1 and \"second_id\" > '2') or (\"first_id\" = 1 and \"second_id\" = '2' and \"third_id\" > '3')) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" asc limit 6");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

  });

  it('asc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 5,
      orderBy: "firstId",
      orderDirection: "asc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"first_id\" asc, \"backup_id\" asc limit 6");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('asc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 5,
      orderBy: "firstId",
      orderDirection: "asc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"first_id\" asc, \"backup_id\" asc limit 6");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('page size', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 10,
      after: base64.encode("2/1"),
      orderBy: "firstId",
      orderDirection: "desc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"backup_id\" < 1)) order by \"first_id\" desc, \"backup_id\" desc limit 11");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('string value in token', async () => {
    const queryStrings = [];
    const cursorInput = {
      last: 7,
      before: base64.encode("\"my%20test%2F\"/1"),
      orderBy: "firstId",
      orderDirection: "desc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 'my test/' or (\"first_id\" = 'my test/' and \"backup_id\" > 1)) order by \"first_id\" desc, \"backup_id\" desc limit 8");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('before', async () => {
    const queryStrings = [];
    const cursorInput = {
      last: 7,
      before: base64.encode("2/1"),
      orderBy: "firstId",
      orderDirection: "desc"
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"backup_id\" > 1)) order by \"first_id\" desc, \"backup_id\" desc limit 8")
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort asc', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    cursorInput.orderDirection = "asc";
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"backup_id\" > 1)) order by \"first_id\" asc, \"backup_id\" asc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('already has filters', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    const query = createTestKnex();
    query.where("otherId", "abc")
    await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where \"other_id\" = 'abc' and (\"first_id\" < 2 or (\"first_id\" = 2 and \"backup_id\" < 1)) order by \"first_id\" desc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\" where \"other_id\" = 'abc'");
  });

  describe('multiple sorts', () => {
    describe('2 columns', () => {
      it('desc then desc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "secondId"],
          orderDirection: ["desc", "desc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" < 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"backup_id\" < 1)) order by \"first_id\" desc, \"second_id\" desc, \"backup_id\" desc limit 4");
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('desc then asc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "secondId"],
          orderDirection: ["desc", "asc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"backup_id\" > 1)) order by \"first_id\" desc, \"second_id\" asc, \"backup_id\" asc limit 4");
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('asc then desc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "secondId"],
          orderDirection: ["asc", "desc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"second_id\" < 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"backup_id\" < 1)) order by \"first_id\" asc, \"second_id\" desc, \"backup_id\" desc limit 4");
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('asc then asc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "secondId"],
          orderDirection: ["asc", "asc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"backup_id\" > 1)) order by \"first_id\" asc, \"second_id\" asc, \"backup_id\" asc limit 4");
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });
    });

    it('3 columns', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/3/4/1"),
        orderBy: ["firstId", "secondId", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" < 4) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" = 4 and \"backup_id\" < 1)) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"backup_id\" desc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('3 columns - no after token', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 6,
        orderBy: ["firstId", "secondId", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"backup_id\" desc limit 7");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('3 columns - before', async () => {
      const queryStrings = [];
      const cursorInput = {
        last: 3,
        before: base64.encode("2/3/4/1"),
        orderBy: ["firstId", "secondId", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"second_id\" < 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" > 4) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" = 4 and \"backup_id\" > 1)) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"backup_id\" desc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('3 columns - other types in tokens', async () => {
      const queryStrings = [];
      const cursorInput = {
        last: 3,
        before: base64.encode("2/true/\"my%20test%2F\"/1"),
        orderBy: ["firstId", "secondId", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"second_id\" < true) or (\"first_id\" = 2 and \"second_id\" = true and \"third_id\" > 'my test/') or (\"first_id\" = 2 and \"second_id\" = true and \"third_id\" = 'my test/' and \"backup_id\" > 1)) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"backup_id\" desc limit 4")
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('4 columns - custom column rendering', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/3/4/5/1"),
        orderBy: ["firstId", "secondId", "thirdId", "fourthId"],
        orderDirection: ["desc", "asc", "desc", "asc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" < 4) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" = 4 and \"fourth_id\" > 5) or (\"first_id\" = 2 and \"second_id\" = 3 and \"third_id\" = 4 and \"fourth_id\" = 5 and \"backup_id\" > 1)) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"fourth_id\" asc, \"backup_id\" asc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });
  });

  describe('null value', () => {
    it('desc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("null/1"),
        orderBy: "firstId",
        orderDirection: "desc",
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where ((\"first_id\" is null and \"backup_id\" < 1)) order by \"first_id\" desc, \"backup_id\" desc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('asc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("null/1"),
        orderBy: "firstId",
        orderDirection: "asc",
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" is not null or (\"first_id\" is null and \"backup_id\" > 1)) order by \"first_id\" asc, \"backup_id\" asc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('two sorts, one is null desc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/null/1"),
        orderBy: ["firstId", "secondId"],
        orderDirection: ["desc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" is null and \"backup_id\" < 1)) order by \"first_id\" desc, \"second_id\" desc, \"backup_id\" desc limit 4");

      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('two sorts, one is null asc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/null/1"),
        orderBy: ["firstId", "secondId"],
        orderDirection: ["desc", "asc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" is not null) or (\"first_id\" = 2 and \"second_id\" is null and \"backup_id\" > 1)) order by \"first_id\" desc, \"second_id\" asc, \"backup_id\" asc limit 4");
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });
  });
});

// Any columns that need aggregates will push all the filters to having.
describe('having', () => {

  it('desc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 4,
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 5");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('desc - no after token', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 4,
      orderBy: ["myMetric1"],
      orderDirection: ["asc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" order by \"sum(my_metric1)\" asc, \"backup_id\" asc limit 5");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('one sort desc', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4")
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('one sort asc', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["asc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" > 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" > 1))) order by \"sum(my_metric1)\" asc, \"backup_id\" asc limit 4")
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort desc', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4");
    // TODO - The total results are wrong when there are group bys.
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('page size', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 10,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 11");
    // TODO - The total results are wrong when there are group bys.
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('page size', async () => {
    const queryStrings = [];
    const cursorInput = {
      last: 7,
      before: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" > 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" > 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 8");
    // TODO - The total results are wrong when there are group bys.
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort asc', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["asc"],
    };
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" > 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" > 1))) order by \"sum(my_metric1)\" asc, \"backup_id\" asc limit 4");
    // TODO - The total results are wrong when there are group bys.
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('already has filters', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("2/1"),
      orderBy: ["myMetric1"],
      orderDirection: ["desc"],
    };
    const query = createTestKnex();
    query.where("otherId", "abc")
    await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where \"other_id\" = 'abc' having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4");
    // TODO - The total results are wrong when there are group bys.
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\" where \"other_id\" = 'abc'");
  });

  describe('multiple sorts', () => {
    describe('2 columns', () => {
      it('desc then desc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "myMetric1"],
          orderDirection: ["desc", "desc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" having ((\"first_id\" < 2 or (\"first_id\" = 2 and \"sum(my_metric1)\" < 3) or (\"first_id\" = 2 and \"sum(my_metric1)\" = 3 and \"backup_id\" < 1))) order by \"first_id\" desc, \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4");
        // TODO - The total results are wrong when there are group bys.
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('desc then asc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["firstId", "myMetric1"],
          orderDirection: ["desc", "asc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" having ((\"first_id\" < 2 or (\"first_id\" = 2 and \"sum(my_metric1)\" > 3) or (\"first_id\" = 2 and \"sum(my_metric1)\" = 3 and \"backup_id\" > 1))) order by \"first_id\" desc, \"sum(my_metric1)\" asc, \"backup_id\" asc limit 4");
        // TODO - The total results are wrong when there are group bys.
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('asc then desc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["myMetric1", "firstId"],
          orderDirection: ["asc", "desc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" > 2 or (\"sum(my_metric1)\" = 2 and \"first_id\" < 3) or (\"sum(my_metric1)\" = 2 and \"first_id\" = 3 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" asc, \"first_id\" desc, \"backup_id\" desc limit 4");
        // TODO - The total results are wrong when there are group bys.
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });

      it('asc then asc', async () => {
        const queryStrings = [];
        const cursorInput = {
          first: 3,
          after: base64.encode("2/3/1"),
          orderBy: ["myMetric", "secondId"],
          orderDirection: ["asc", "asc"],
        };
        const query = createTestKnex();
        await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
        expect(queryStrings.length).toEqual(2);
        expect(queryStrings[0])
          .toEqual("select \"metric\" from \"mytable\" where (\"my_metric\" > 2 or (\"my_metric\" = 2 and \"second_id\" > 3) or (\"my_metric\" = 2 and \"second_id\" = 3 and \"backup_id\" > 1)) order by \"my_metric\" asc, \"second_id\" asc, \"backup_id\" asc limit 4");
        // TODO - The total results are wrong when there are group bys.
        expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
      });
    });

    it('3 columns', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/3/4/1"),
        orderBy: ["myMetric1", "myMetric2", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" > 3) or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" = 3 and \"third_id\" < 4) or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" = 3 and \"third_id\" = 4 and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"sum(my_metric2)\" asc, \"third_id\" desc, \"backup_id\" desc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('3 columns - no after token', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        orderBy: ["myMetric1", "myMetric2", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" order by \"sum(my_metric1)\" desc, \"sum(my_metric2)\" asc, \"third_id\" desc, \"backup_id\" desc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('3 columns - before', async () => {
      const queryStrings = [];
      const cursorInput = {
        last: 3,
        before: base64.encode("2/3/4/1"),
        orderBy: ["myMetric1", "myMetric2", "thirdId"],
        orderDirection: ["desc", "asc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" > 2 or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" < 3) or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" = 3 and \"third_id\" > 4) or (\"sum(my_metric1)\" = 2 and \"sum(my_metric2)\" = 3 and \"third_id\" = 4 and \"backup_id\" > 1))) order by \"sum(my_metric1)\" desc, \"sum(my_metric2)\" asc, \"third_id\" desc, \"backup_id\" desc limit 4")
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('4 columns - custom column rendering', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/3/4/5/1"),
        orderBy: ["firstId", "myMetric1", "thirdId", "fourthId"],
        orderDirection: ["desc", "asc", "desc", "asc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"first_id\" < 2 or (\"first_id\" = 2 and \"sum(my_metric1)\" > 3) or (\"first_id\" = 2 and \"sum(my_metric1)\" = 3 and \"third_id\" < 4) or (\"first_id\" = 2 and \"sum(my_metric1)\" = 3 and \"third_id\" = 4 and \"fourth_id\" > 5) or (\"first_id\" = 2 and \"sum(my_metric1)\" = 3 and \"third_id\" = 4 and \"fourth_id\" = 5 and \"backup_id\" > 1))) order by \"first_id\" desc, \"sum(my_metric1)\" asc, \"third_id\" desc, \"fourth_id\" asc, \"backup_id\" asc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });
  });

  describe('null value', () => {

    // I don't think this can happen but we'll just keep the test in case our SQL result is somehow null.
    it('desc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("null/1"),
        orderBy: "myMetric1",
        orderDirection: "desc",
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having (((\"sum(my_metric1)\" is null and \"backup_id\" < 1))) order by \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    // I don't think this can happen but we'll just keep the test in case our SQL result is somehow null.
    it('asc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("null/1"),
        orderBy: "myMetric1",
        orderDirection: "asc",
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" is not null or (\"sum(my_metric1)\" is null and \"backup_id\" > 1))) order by \"sum(my_metric1)\" asc, \"backup_id\" asc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('two sorts, one is null desc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/null/1"),
        orderBy: ["firstId", "myMetric1"],
        orderDirection: ["desc", "desc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"first_id\" < 2 or (\"first_id\" = 2 and \"sum(my_metric1)\" is null and \"backup_id\" < 1))) order by \"first_id\" desc, \"sum(my_metric1)\" desc, \"backup_id\" desc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });

    it('two sorts, one is null asc', async () => {
      const queryStrings = [];
      const cursorInput = {
        first: 3,
        after: base64.encode("2/null/1"),
        orderBy: ["myMetric1", "secondId"],
        orderDirection: ["desc", "asc"],
      };
      const query = createTestKnex();
      await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
      expect(queryStrings.length).toEqual(2);
      expect(queryStrings[0])
        .toEqual("select \"metric\" from \"mytable\" having ((\"sum(my_metric1)\" < 2 or (\"sum(my_metric1)\" = 2 and \"second_id\" is not null) or (\"sum(my_metric1)\" = 2 and \"second_id\" is null and \"backup_id\" > 1))) order by \"sum(my_metric1)\" desc, \"second_id\" asc, \"backup_id\" asc limit 4");
      // TODO - The total results are wrong when there are group bys.
      expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
    });
  });

  // TODO - get sorting to work on true/false.
  // TODO - make sure bigint IDs work.

  // TODO - do where pagination when there is already a having.
  // TODO - do having pagination when there is already a having.

});
