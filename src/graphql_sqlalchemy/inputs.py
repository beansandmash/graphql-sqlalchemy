from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLInputField,
    GraphQLInputFieldMap,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString,
)
from sqlalchemy import Float, Integer

from typing import Union, Dict, cast
from .graphql_types import get_graphql_type_from_column
from .helpers import get_relationships, get_table
from .names import get_field_name
from .types import Inputs
from sqlalchemy.ext.declarative import DeclarativeMeta


ORDER_BY_ENUM = GraphQLEnumType("order_by", {"desc": "desc", "asc": "asc"})


def get_empty_dict() -> Dict[str, GraphQLInputField]:
    return {}


def get_type_comparison_fields(graphql_type: Union[GraphQLScalarType, GraphQLList], inputs: Inputs, type_name: str) -> GraphQLInputObjectType:
    if type_name in inputs:
        return inputs[type_name]

    fields = {
        "_eq": GraphQLInputField(graphql_type),
        "_neq": GraphQLInputField(graphql_type),
        "_in": GraphQLInputField(GraphQLList(GraphQLNonNull(graphql_type))),
        "_nin": GraphQLInputField(GraphQLList(GraphQLNonNull(graphql_type))),
        "_lt": GraphQLInputField(graphql_type),
        "_gt": GraphQLInputField(graphql_type),
        "_gte": GraphQLInputField(graphql_type),
        "_lte": GraphQLInputField(graphql_type),
        "_is_null": GraphQLInputField(GraphQLBoolean),
    }

    fields_string = {
        "_like": GraphQLInputField(GraphQLString),
        "_nlike": GraphQLInputField(GraphQLString),
    }

    if graphql_type == GraphQLString:
        fields.update(fields_string)

    inputs[type_name] = GraphQLInputObjectType(type_name, fields)
    return inputs[type_name]


def get_input_type(model: DeclarativeMeta, inputs: Inputs, input_type: str) -> GraphQLInputObjectType:
    type_name = get_field_name(model, input_type)

    """ skip if field already exists """
    if type_name in inputs:
        return inputs[type_name]

    def get_fields() -> GraphQLInputFieldMap:
        """ initial field population """
        input_field1 = {
            "where": {
                "_and": GraphQLInputField(GraphQLList(inputs[type_name])),
                "_or": GraphQLInputField(GraphQLList(inputs[type_name])),
                "_not": GraphQLInputField(inputs[type_name]),
            },
            "on_conflict": {
                "merge": GraphQLInputField(GraphQLNonNull(GraphQLBoolean)),
            },
        }

        if input_type in input_field1.keys():
            fields = input_field1[input_type]
        else:
            fields = get_empty_dict()

        """ per column population """
        for column in get_table(model).columns:
            graphql_type = get_graphql_type_from_column(column.type)
            column_type = GraphQLInputField(graphql_type)

            input_field2 = {
                "where": GraphQLInputField(get_type_comparison_fields(graphql_type, inputs, get_field_name(graphql_type, "comparison"))),
                "order_by": GraphQLInputField(ORDER_BY_ENUM),
                "insert_input": column_type,
                "inc_input": column_type if isinstance(column.type, (Integer, Float)) else None,
                "set_input": column_type,
            }

            if input_type in input_field2.keys() and input_field2[input_type]:
                fields[column.name] = cast(GraphQLInputField, input_field2[input_type])

        """ relationship population """
        for name, relationship in get_relationships(model):
            input_field3 = {
                "where": GraphQLInputField(inputs[get_field_name(relationship.mapper.entity, "where")]),
                "order_by": GraphQLInputField(inputs[get_field_name(relationship.mapper.entity, "order_by")]),
            }

            if input_type in input_field3.keys():
                fields[name] = input_field3[input_type]

        return fields

    inputs[type_name] = GraphQLInputObjectType(type_name, get_fields)
    return inputs[type_name]
