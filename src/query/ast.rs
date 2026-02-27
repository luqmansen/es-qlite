/// Elasticsearch Query DSL AST
#[derive(Debug, Clone)]
pub enum Query {
    MatchAll,
    Match {
        field: String,
        query: String,
        operator: BoolOperator,
    },
    Term {
        field: String,
        value: serde_json::Value,
    },
    Terms {
        field: String,
        values: Vec<serde_json::Value>,
    },
    Range {
        field: String,
        gte: Option<serde_json::Value>,
        gt: Option<serde_json::Value>,
        lte: Option<serde_json::Value>,
        lt: Option<serde_json::Value>,
    },
    Bool {
        must: Vec<Query>,
        should: Vec<Query>,
        must_not: Vec<Query>,
        filter: Vec<Query>,
        minimum_should_match: Option<usize>,
    },
    MultiMatch {
        query: String,
        fields: Vec<String>,
        match_type: MultiMatchType,
    },
    Exists {
        field: String,
    },
    MatchNone,
    QueryString {
        query: String,
        fields: Vec<String>,
        default_operator: BoolOperator,
    },
    Wildcard {
        field: String,
        value: String,
    },
    Prefix {
        field: String,
        value: String,
    },
}

#[derive(Debug, Clone, Default)]
pub enum BoolOperator {
    #[default]
    Or,
    And,
}

#[derive(Debug, Clone, Default)]
pub enum MultiMatchType {
    #[default]
    BestFields,
    MostFields,
    CrossFields,
    Phrase,
    PhrasePrefix,
}
