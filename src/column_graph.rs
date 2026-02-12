use crate::identifier::{ColumnIdentifier, Identifier, ParseIdentifierError, TableIdentifier};
use crate::import_options::ImportOptions;
use crate::table_mapper::{
    Column, FieldColumn, LookupColumn, LookupKeyColumn, ParserColumn, ParserKeyColumn,
    ProcessedKeyColumn, StaticColumn, TableMapper, TableMapperColumn,
};
use itertools::Itertools;
use log::{Level, debug, log_enabled};
use petgraph::Direction;
use petgraph::algo::ToposortGroupingStrategy::Eager;
use petgraph::dot::{Config, Dot};
use petgraph::graph::{DefaultIx, NodeIndex};
use petgraph::prelude::StableDiGraph;
use petgraph::stable_graph::EdgeIndex;
use petgraph::visit::IntoNodeReferences;
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::str::FromStr;
use thiserror::Error;
use tiberius::{BaseMetaDataColumn, TypeInfo, VarLenContext, VarLenType};

/* Directed graph of ColumnNode, unit struct edge weights, default index (u32). */
type ColumnGraphType = StableDiGraph<ColumnNode, (), DefaultIx>;

#[derive(Debug)]
pub struct ColumnGraph {
    graph: ColumnGraphType,
    groups: Vec<Vec<NodeIndex>>,
    unique_identifiers: HashMap<NodeIndex, UniqueColumnIdentifier>,
    metadata: HashMap<NodeIndex, BaseMetaDataColumn>,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum ColumnNode {
    StaticColumn {
        column: StaticColumn,
        map_column: bool,
    },
    ParserColumn {
        column: ParserColumn,
        map_column: bool,
    },
    LookupColumn {
        column: LookupColumn,
        map_column: bool,
    },
    LookupColumnParserKeyColumn(ParserKeyColumn),
    LookupColumnProcessedKeyColumn(ProcessedKeyColumn),
}

#[derive(Debug, Clone, Copy)]
pub struct IndexedColumnNode<'a> {
    index: NodeIndex,
    column: &'a ColumnNode,
    unique_identifier: &'a UniqueColumnIdentifier,
    metadata: &'a BaseMetaDataColumn,
}

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct UniqueColumnIdentifier(ColumnIdentifier);

#[derive(Debug, Error)]
pub enum CreateColumnGraphError {
    #[error("could not find column target for processed key column: {0}")]
    ProcessedKeyColumnUnknownTargetColumn(ColumnIdentifier),
    #[error("could not topologically sort column graph due to cycle with column: {0}")]
    ColumnCycle(ColumnIdentifier),
}

impl ColumnGraph {
    pub fn new(
        table_mapper: &TableMapper,
        table_metadata: &HashMap<&TableIdentifier, HashMap<ColumnIdentifier, BaseMetaDataColumn>>,
        import_options: &ImportOptions,
    ) -> Result<ColumnGraph, CreateColumnGraphError> {
        let mut graph = ColumnGraphType::with_capacity(table_mapper.columns().len(), 0);

        // do not add edges from processed key columns to dynamically added columns
        let mut dynamic_column_indices: HashSet<NodeIndex> = HashSet::default();

        // add graph nodes
        for column in table_mapper.columns() {
            match column {
                TableMapperColumn::Static(static_column) => {
                    graph.add_node(ColumnNode::StaticColumn {
                        column: static_column.clone(),
                        map_column: false,
                    });
                }
                TableMapperColumn::Parser(parser_column) => {
                    graph.add_node(ColumnNode::ParserColumn {
                        column: parser_column.clone(),
                        map_column: false,
                    });
                }
                TableMapperColumn::Lookup(lookup_column) => {
                    let lookup_column_index = graph.add_node(ColumnNode::LookupColumn {
                        column: lookup_column.clone(),
                        map_column: false,
                    });

                    for lookup_key_column in lookup_column.iter_key_columns() {
                        match lookup_key_column {
                            LookupKeyColumn::ParserKeyColumn(parser_key_column) => {
                                let parser_key_column_index =
                                    graph.add_node(ColumnNode::LookupColumnParserKeyColumn(
                                        parser_key_column.clone(),
                                    ));

                                graph.add_edge(parser_key_column_index, lookup_column_index, ());

                                let parser_key_column_target_index =
                                    graph.add_node(ColumnNode::ParserColumn {
                                        column: ParserColumn::new(
                                            parser_key_column.identifier(),
                                            false,
                                            parser_key_column.field_name(),
                                        ),
                                        map_column: false,
                                    });

                                graph.add_edge(
                                    parser_key_column_target_index,
                                    parser_key_column_index,
                                    (),
                                );

                                dynamic_column_indices.insert(parser_key_column_target_index);
                            }
                            LookupKeyColumn::ProcessedKeyColumn(processed_key_column) => {
                                let processed_key_column_index =
                                    graph.add_node(ColumnNode::LookupColumnProcessedKeyColumn(
                                        processed_key_column.clone(),
                                    ));

                                graph.add_edge(processed_key_column_index, lookup_column_index, ());
                            }
                        }
                    }
                }
            }
        }

        // add graph edges for processed key columns
        for (a, b) in graph
            .node_references()
            .map(|(column_index, column)| match column {
                ColumnNode::LookupColumnProcessedKeyColumn(processed_key_column) => {
                    let processed_key_column_target_index = graph
                        .node_references()
                        .find_map(|(column_index, column)| match column {
                            ColumnNode::StaticColumn { .. }
                            | ColumnNode::ParserColumn { .. }
                            | ColumnNode::LookupColumn { .. }
                                if !dynamic_column_indices.contains(&column_index)
                                    && column.identifier() == processed_key_column.identifier() =>
                            {
                                Some(column_index)
                            }
                            _ => None,
                        })
                        .ok_or_else(|| {
                            CreateColumnGraphError::ProcessedKeyColumnUnknownTargetColumn(
                                processed_key_column.identifier().to_owned(),
                            )
                        })?;

                    Ok(Some((processed_key_column_target_index, column_index)))
                }
                _ => Ok(None),
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<(NodeIndex, NodeIndex)>, CreateColumnGraphError>>()?
        {
            graph.add_edge(a, b, ());
        }

        if !import_options.no_duplicate_optimization {
            let duplicate_groups: Vec<Vec<NodeIndex>> = graph
                .node_references()
                .map(|(a, b)| (b, a))
                .into_group_map()
                .into_values()
                .collect();

            let mut msg = format!(
                "Applied duplicate optimization for table mapper {}:\n",
                table_mapper.name()
            );

            for duplicate_group in &duplicate_groups {
                if let [first_duplicate, nth_duplicates @ ..] = duplicate_group.as_slice()
                    && !nth_duplicates.is_empty()
                {
                    writeln!(
                        msg,
                        "node \"{}\", count {}",
                        graph[*first_duplicate],
                        nth_duplicates.len()
                    )
                    .unwrap();

                    for nth_duplicate in nth_duplicates {
                        for edge in graph.edge_indices().collect::<Vec<EdgeIndex>>() {
                            if let Some((source, target)) = graph.edge_endpoints(edge) {
                                if source == *nth_duplicate {
                                    graph.update_edge(*first_duplicate, target, ());
                                } else if target == *nth_duplicate {
                                    graph.update_edge(source, *first_duplicate, ());
                                }
                            } else {
                                unreachable!("Edge always exists")
                            }
                        }

                        let map_nth_duplicate = graph[*nth_duplicate].map();

                        match &mut graph[*first_duplicate] {
                            ColumnNode::StaticColumn { map_column, .. } => {
                                *map_column = *map_column || map_nth_duplicate
                            }
                            ColumnNode::ParserColumn { map_column, .. } => {
                                *map_column = *map_column || map_nth_duplicate
                            }
                            ColumnNode::LookupColumn { map_column, .. } => {
                                *map_column = *map_column || map_nth_duplicate
                            }
                            _ => {}
                        };

                        graph.remove_node(*nth_duplicate);
                    }
                }
            }

            if duplicate_groups
                .iter()
                .any(|duplicate_group| duplicate_group.len() >= 2)
            {
                debug!("{}", msg.trim());
            }
        }

        graph.shrink_to_fit();

        if log_enabled!(Level::Debug) {
            let mut msg = format!(
                "Constructed graph for table mapper {}:\n",
                table_mapper.name(),
            );

            write!(
                msg,
                "{:?}",
                Dot::with_attr_getters(
                    &graph,
                    &[
                        Config::EdgeNoLabel,
                        Config::NodeNoLabel,
                        Config::EdgeIndexLabel
                    ],
                    &|_, _| String::default(),
                    &|_, (_, n)| format!(r#"label = "{}""#, n),
                )
            )
            .unwrap();

            debug!("{}", msg.trim());
        }

        let groups = petgraph::algo::toposort_grouped(&graph, Eager).map_err(|cycle| {
            CreateColumnGraphError::ColumnCycle(graph[cycle.node_id()].identifier().to_owned())
        })?;

        if log_enabled!(Level::Debug) {
            let mut msg = format!(
                "Topologically sorted graph for table mapper {}:\n",
                table_mapper.name()
            );

            for (group_index, group) in groups.iter().enumerate() {
                for index in group.iter() {
                    let node = &graph[*index];

                    writeln!(msg, "group {}, node \"{}\"", group_index, node).unwrap();
                }
            }

            debug!("{}", msg.trim());
        }

        // map unique identifiers for each column
        let unique_identifiers = Self::build_unique_identifiers(&graph);

        // map metadata for each column
        let metadata = Self::build_metadata(&graph, table_metadata);

        Ok(ColumnGraph {
            graph,
            groups,
            unique_identifiers,
            metadata,
        })
    }

    fn build_unique_identifiers(
        graph: &ColumnGraphType,
    ) -> HashMap<NodeIndex, UniqueColumnIdentifier> {
        graph
            .node_references()
            .map(|(column_index, column)| {
                let mut unique_name = String::with_capacity(
                    column.identifier().part_unescaped().len()
                        + 2
                        + NonZeroUsize::new(column_index.index())
                            .unwrap_or(NonZeroUsize::MIN)
                            .ilog10() as usize,
                );

                write!(
                    &mut unique_name,
                    "{}_{}",
                    column.identifier().part_unescaped(),
                    column_index.index(),
                )
                .expect("Write to string should be infallible");

                (
                    column_index,
                    UniqueColumnIdentifier(
                        ColumnIdentifier::with_table(&column.identifier().into(), &unique_name)
                            .expect("Unique column identifiers should be valid"),
                    ),
                )
            })
            .collect::<HashMap<NodeIndex, UniqueColumnIdentifier>>()
    }

    fn build_metadata(
        graph: &ColumnGraphType,
        table_metadata: &HashMap<&TableIdentifier, HashMap<ColumnIdentifier, BaseMetaDataColumn>>,
    ) -> HashMap<NodeIndex, BaseMetaDataColumn> {
        graph
            .node_references()
            .map(|(column_index, column)| {
                let default_metadata = BaseMetaDataColumn {
                    flags: Default::default(),
                    ty: TypeInfo::VarLenSized(VarLenContext::new(
                        VarLenType::NVarchar,
                        usize::MAX,
                        None,
                    )),
                };

                let column_schema = table_metadata
                    .get(&TableIdentifier::from(column.identifier()))
                    .expect("Table metadata should not be missing")
                    .get(column.identifier())
                    .unwrap_or(&default_metadata);

                (column_index, column_schema.to_owned())
            })
            .collect::<HashMap<NodeIndex, BaseMetaDataColumn>>()
    }

    pub fn target_columns(&self) -> impl Iterator<Item = IndexedColumnNode<'_>> {
        self.graph.node_references().filter_map(|(index, column)| {
            if column.map() {
                Some(IndexedColumnNode {
                    index,
                    column,
                    unique_identifier: &self.unique_identifiers[&index],
                    metadata: &self.metadata[&index],
                })
            } else {
                None
            }
        })
    }

    pub fn groups(
        &self,
    ) -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = IndexedColumnNode<'_>>> {
        self.groups.iter().map(|group| {
            group.iter().map(|&index| IndexedColumnNode {
                index,
                column: &self.graph[index],
                unique_identifier: &self.unique_identifiers[&index],
                metadata: &self.metadata[&index],
            })
        })
    }

    pub fn column_dependencies(
        &self,
        index: &NodeIndex,
    ) -> impl Iterator<Item = IndexedColumnNode<'_>> {
        self.graph
            .neighbors_directed(*index, Direction::Incoming)
            .map(|node| IndexedColumnNode {
                index: node,
                column: &self.graph[node],
                unique_identifier: &self.unique_identifiers[&node],
                metadata: &self.metadata[&node],
            })
    }
}

impl ColumnNode {
    pub fn is_transient(&self) -> bool {
        match self {
            ColumnNode::StaticColumn { .. } => true,
            ColumnNode::ParserColumn { .. } => false,
            ColumnNode::LookupColumn { .. } => false, // TODO: this could be true, sometimes
            // lookup key columns will not be transient if they have regex formatters / validators
            ColumnNode::LookupColumnParserKeyColumn(_) => true,
            ColumnNode::LookupColumnProcessedKeyColumn(_) => true,
        }
    }
}

impl<'a> IndexedColumnNode<'a> {
    pub fn index(&self) -> &NodeIndex {
        &self.index
    }

    pub fn column(&self) -> &'a ColumnNode {
        self.column
    }

    pub fn unique_identifier(&self) -> &'a UniqueColumnIdentifier {
        self.unique_identifier
    }

    pub fn metadata(&self) -> &'a BaseMetaDataColumn {
        self.metadata
    }
}

impl Display for ColumnNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnNode::StaticColumn { column, .. } => {
                write!(f, "static column {}", Column::identifier(column))
            }
            ColumnNode::ParserColumn { column, .. } => {
                write!(f, "parser column {}", Column::identifier(column))
            }
            ColumnNode::LookupColumn { column, .. } => {
                write!(f, "lookup column {}", Column::identifier(column))
            }
            ColumnNode::LookupColumnParserKeyColumn(parser_key_column) => {
                write!(
                    f,
                    "parser key column {}",
                    Column::identifier(parser_key_column)
                )
            }
            ColumnNode::LookupColumnProcessedKeyColumn(processed_key_column) => write!(
                f,
                "processed key column {}",
                Column::identifier(processed_key_column)
            ),
        }
    }
}

impl FromStr for UniqueColumnIdentifier {
    type Err = ParseIdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(UniqueColumnIdentifier(s.parse()?))
    }
}

impl Identifier for UniqueColumnIdentifier {
    fn part(&self) -> &str {
        self.0.part()
    }

    fn full(&self) -> &str {
        self.0.full()
    }
}

impl Column for ColumnNode {
    fn identifier(&self) -> &ColumnIdentifier {
        match self {
            ColumnNode::StaticColumn { column, .. } => Column::identifier(column),
            ColumnNode::ParserColumn { column, .. } => Column::identifier(column),
            ColumnNode::LookupColumn { column, .. } => Column::identifier(column),
            ColumnNode::LookupColumnParserKeyColumn(lookup_column_parser_key_column) => {
                Column::identifier(lookup_column_parser_key_column)
            }
            ColumnNode::LookupColumnProcessedKeyColumn(lookup_column_processed_key_column) => {
                Column::identifier(lookup_column_processed_key_column)
            }
        }
    }

    fn map(&self) -> bool {
        match self {
            ColumnNode::StaticColumn { column, map_column } => *map_column || Column::map(column),
            ColumnNode::ParserColumn { column, map_column } => *map_column || Column::map(column),
            ColumnNode::LookupColumn { column, map_column } => *map_column || Column::map(column),
            ColumnNode::LookupColumnParserKeyColumn(_) => false,
            ColumnNode::LookupColumnProcessedKeyColumn(_) => false,
        }
    }
}
