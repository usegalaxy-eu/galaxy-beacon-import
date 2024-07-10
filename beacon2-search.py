from pymongo import MongoClient
import argparse
import pprint
import sys
import logging

def common_arguments(parser):
    connection_group = parser.add_argument_group("Connection to MongoDB")
    connection_group.add_argument("-H", "--db-host", type=str, default="127.0.0.1", dest="database_host", help="hostname/IP of the beacon database")
    connection_group.add_argument("-P", "--db-port", type=int, default=27017, dest="database_port", help="port of the beacon database")
    
    advance_connection_group = parser.add_argument_group("Advanced Connection to MongoDB")
    advance_connection_group.add_argument('-a', '--advance-connection', action="store_true", dest="advance", default=False, help="Connect to beacon database with authentication")
    advance_connection_group.add_argument("-A", "--db-auth-source", type=str, metavar="ADMIN", default="admin", dest="database_auth_source", help="auth source for the beacon database")
    advance_connection_group.add_argument("-U", "--db-user", type=str, default="", dest="database_user", help="login user for the beacon database")
    advance_connection_group.add_argument("-W", "--db-password", type=str, default="", dest="database_password", help="login password for the beacon database")

    database_group = parser.add_argument_group("Database Configuration")
    database_group.add_argument("-d", "--database", type=str, default="", dest="database", help="The targeted beacon database")
    database_group.add_argument("-c", "--collection", type=str, default="", dest="collection", help="The targeted beacon collection from the desired database")

def connect_to_mongodb(args):
    if args.advance:
        advanced_required_args = ['database_auth_source', 'database_user', 'database_password']
        if any(getattr(args, arg) == "" for arg in advanced_required_args):
            for arg in advanced_required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    logging.info(f"Missing value -> {arg}")
            parser.print_help()
            sys.exit(1)
        client = MongoClient(f"mongodb://{args.database_user}:{args.database_password}@{args.database_host}:{args.database_port}/{args.database}?authSource={args.database_auth_source}")
    else:
        client = MongoClient(args.database_host, args.database_port)
    return client

def beacon_query():

    """
    Beacon Query Tool

    This script provides a command-line interface for querying a Beacon Database
    using various sub-commands for sequence, range, gene ID, or bracket criteria.

    Example Usage:

    1. Query by sequence:
        beacon_search sequence -d database_name -c collection_name -rn  -s  -ab 

    2. Query by range:
        beacon_search range -d database_name -c collection_name -rn  -s  -e  -v 

    3. Query by gene ID:
        beacon_search gene -d database_name -c collection_name -g  -vmin   -vmax 

    4. Query by bracket:
        beacon_search bracket -d database_name -c collection_name -rn  -smin  -smax  -emin  -emax  -v 
    """
    parser = argparse.ArgumentParser(description="Query Beacon Database")
    subparsers = parser.add_subparsers(dest="command")
    parsers = {}
    # subparsers.required = True

    # Sub-parser for command "Beacon Sequence Queries"
    parser_sequence = subparsers.add_parser("sequence", help="Connect to MongoDB and perform sequence-based queries")
    common_arguments(parser_sequence)
    parsers["sequence"] = parser_sequence

    # Positional Search Query Parameters
    query_group = parser_sequence.add_argument_group("Positional Database Query Arguments")
    query_group.add_argument("-ab", "--alternateBases", type=str, default="", dest="alternateBases", help="Alternate bases")
    query_group.add_argument("-rb", "--referenceBases", type=str, default="", dest="referenceBases", help="Reference bases")
    # Optional Search Query Parameters
    optional_query_group = parser_sequence.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-rn", "--referenceName", type=str, default="", dest="referenceName", help="Reference name")
    optional_query_group.add_argument("-s", "--start", type=int, default=None, dest="start", help="Start position")
    optional_query_group.add_argument("-id", "--collectionIds", type=str, default="", dest="collectionIds", help="Collection ID")
    
    # Sub-parser for command "Beacon Range Queries"
    parser_range = subparsers.add_parser("range", help="Connect to MongoDB and perform range-based queries")
    common_arguments(parser_range)
    parsers["range"] = parser_range
    
    # Positional Search Query Parameters
    query_group = parser_range.add_argument_group("Positional Database Query Arguments")
    query_group.add_argument("-s", "--start", type=int, default=None, dest="start", help="Start position")
    query_group.add_argument("-e", "--end", type=int, default=None, dest="end", help="End position")
    # Optional Search Query Parameters
    optional_query_group = parser_range.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-rn", "--referenceName", type=str, default="", dest="referenceName", help="Reference name")
    optional_query_group.add_argument("-ab", "--alternateBases", type=str, default="", dest="alternateBases", help="Alternate bases")
    optional_query_group.add_argument("-v", "--variantType", type=str, default="", dest="variantType", help="Variant type")
    optional_query_group.add_argument("-ac", "--aminoacidChange", type=str, default="", dest="aminoacidChange", help="Amino acid change")
    optional_query_group.add_argument("-vmax", "--variantMinLength", type=int, default=None, dest="variantMinLength", help="Variant minimum length")
    optional_query_group.add_argument("-vmin", "--variantMaxLength", type=int, default=None, dest="variantMaxLength", help="Variant maximum length")
    
    # Sub-parser for command "Beacon GeneId Queries"
    parser_gene = subparsers.add_parser("gene", help="Connect to MongoDB and perform geneID-based queries")
    common_arguments(parser_gene)
    parsers["gene"] = parser_gene
    # Positional Search Query Parameters
    query_group = parser_gene.add_argument_group("Positional Database Query Arguments")
    query_group.add_argument("-g", "--geneId", type=str, default="", dest="geneId", help="Gene ID")
    # Optional Search Query Parameters
    optional_query_group = parser_gene.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-ab", "--alternateBases", type=str, default="", dest="alternateBases", help="Alternate bases")
    optional_query_group.add_argument("-v", "--variantType", type=str, default="", dest="variantType", help="Variant type")
    optional_query_group.add_argument("-ac", "--aminoacidChange", type=str, default="", dest="aminoacidChange", help="Amino acid change")
    optional_query_group.add_argument("-vmax", "--variantMinLength", type=int, default=None, dest="variantMinLength", help="Variant minimum length")
    optional_query_group.add_argument("-vmin", "--variantMaxLength", type=int, default=None, dest="variantMaxLength", help="Variant maximum length")
    
    # Sub-parser for command "Beacon Bracket Queries"

    # Sub-parser for command "Beacon Bracket Queries"
    parser_bracket = subparsers.add_parser("bracket", help="Connect to MongoDB and perform bracket-based queries")
    common_arguments(parser_bracket)
    parsers["bracket"] = parser_bracket
    
    # Search Query Parameters
    query_group = parser_bracket.add_argument_group("Positional Database Query Arguments")
    query_group.add_argument("-smin", "--start-minimum", type=int, default=None, dest="start_minimum", help="Start minimum position")
    query_group.add_argument("-smax", "--start-maximum", type=int, default=None, dest="start_maximum", help="Start maximum position")
    query_group.add_argument("-emin", "--end-minimum", type=int, default=None, dest="end_minimum", help="End minimum position")
    query_group.add_argument("-emax", "--end-maximum", type=int, default=None, dest="end_maximum", help="End maximum position")
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_bracket.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-rn", "--referenceName", type=str, default="", dest="referenceName", help="Reference name")
    optional_query_group.add_argument("-v", "--variantType", type=str, default="", dest="variantType", help="Variant type")
    
    args = parser.parse_args()
    # Check if a sub-command has been provided
    if args.command is None:
        print("Please provide a valid sub-command. Use -h or --help for usage details.")
        parser.print_help()
        sys.exit(1)  # exit with an error code
    
    # query sequence_queries
    if args.command == "sequence":
        required_args = ['database', 'collection', 'database_host', 'database_port','alternateBases','referenceBases']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        possible_fields = [
            {"referenceName": "referenceName", "start": "start", "alternateBases": "alternateBases", "referenceBases": "referenceBases", "collectionIds": "collectionIds"},
            {"referenceName": "variation.location.sequence_id", "start": "variation.location.interval.start.value", "alternateBases": "variation.alternateBases", "referenceBases": "variation.referenceBases", "collectionIds": "caseLevelData.biosampleId"}
            # Add other potential field mappings as needed
        ]
        
        or_conditions = []
        
        for fields in possible_fields:
            condition = {}
            if args.referenceName:
                condition[fields["referenceName"]] = args.referenceName
            if args.start is not None:
                condition[fields["start"]] = args.start
            if args.alternateBases:
                condition[fields["alternateBases"]] = args.alternateBases
            if args.referenceBases:
                condition[fields["referenceBases"]] = args.referenceBases
            if args.collectionIds:
                condition[fields["collectionIds"]] = {"$in": args.collectionIds}
        
            # Only add non-empty conditions to or_conditions
            if condition:
                or_conditions.append(condition)
        
        # Create the query only if or_conditions is not empty
        query = {"$or": or_conditions} if or_conditions else {}

        # Debugging print statement to verify the constructed query
        print("Constructed query:", query)

    elif args.command == "range":
        required_args = ['database', 'collection', 'database_host', 'database_port','start','end']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        
        possible_fields = [
            {"referenceName": "referenceName", "start": "start", "end": "end", "variantType": "variantType", "alternateBases": "alternateBases", "aminoacidChange": "aminoacidChange", "variantMinLength": "variantMinLength", "variantMaxLength": "variantMaxLength"},
            {"referenceName": "variation.location.sequence_id", "start": "variation.location.interval.start.value", "end": "variation.location.interval.end.value", "variantType": "variation.variantType", "alternateBases": "variation.alternateBases", "aminoacidChange": "molecularAttributes.aminoacidChanges", "variantMinLength": "variation.location.interval.start.value", "variantMaxLength": "variation.location.interval.end.value"}
            # Add other potential field mappings as needed
        ]
        
        or_conditions = []
        
        for fields in possible_fields:
            condition = {}
            if args.referenceName:
                condition[fields["referenceName"]] = args.referenceName
            if args.start is not None:
                condition[fields["start"]] = args.start
            if args.end is not None:
                condition[fields["end"]] = args.end
            if args.variantType:
                condition[fields["variantType"]] = args.variantType
            if args.alternateBases:
                condition[fields["alternateBases"]] = args.alternateBases
            if args.aminoacidChange:
                condition[fields["aminoacidChange"]] = args.aminoacidChange
            if args.variantMinLength is not None:
                condition[fields["variantMinLength"]] = {"$gte": args.variantMinLength}
            if args.variantMaxLength is not None:
                condition[fields["variantMaxLength"]] = {"$lte": args.variantMaxLength}
        
            # Only add non-empty conditions to or_conditions
            if condition:
                or_conditions.append(condition)
        
        # Create the query only if or_conditions is not empty
        query = {"$or": or_conditions} if or_conditions else {}


    # query gene_id_queries
    elif args.command == "gene":
        required_args = ['database', 'collection', 'database_host', 'database_port','geneId']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        possible_fields = [
            {"geneId": "geneId", "variantType": "variantType", "alternateBases": "alternateBases", "aminoacidChange": "aminoacidChange", "variantMinLength": "variantMinLength", "variantMaxLength": "variantMaxLength"},
            {"geneId": "molecularAttributes.geneIds", "variantType": "variation.variantType", "alternateBases": "variation.alternateBases", "aminoacidChange": "molecularAttributes.aminoacidChanges", "variantMinLength": "variation.location.interval.start.value", "variantMaxLength": "variation.location.interval.end.value"}
            # Add other potential field mappings as needed
        ]
        
        or_conditions = []
        
        for fields in possible_fields:
            condition = {}
            if args.geneId:
                condition[fields["geneId"]] = args.geneId
            if args.variantType:
                condition[fields["variantType"]] = args.variantType
            if args.alternateBases:
                condition[fields["alternateBases"]] = args.alternateBases
            if args.aminoacidChange:
                condition[fields["aminoacidChange"]] = args.aminoacidChange
            if args.variantMinLength is not None:
                condition[fields["variantMinLength"]] = {"$gte": args.variantMinLength}
            if args.variantMaxLength is not None:
                condition[fields["variantMaxLength"]] = {"$lte": args.variantMaxLength}
        
            # Only add non-empty conditions to or_conditions
            if condition:
                or_conditions.append(condition)
        
        # Create the query only if or_conditions is not empty
        query = {"$or": or_conditions} if or_conditions else {}


    # query bracket_queries
    elif args.command == "bracket":
        required_args = ['database', 'collection', 'database_host', 'database_port','start_minimum','start_maximum','end_minimum','end_maximum']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        possible_fields = [
            {"referenceName": "referenceName", "start": "start", "end": "end", "variantType": "variantType"},
            {"referenceName": "variation.location.sequence_id", "start": "variation.location.interval.start.value", "end": "variation.location.interval.end.value", "variantType": "variation.variantType"}
            # Add other potential field mappings as needed
        ]
        
        or_conditions = []
        
        for fields in possible_fields:
            condition = {}
            if args.referenceName:
                condition[fields["referenceName"]] = args.referenceName
            if args.start_minimum is not None and args.start_maximum is not None:
                condition[fields["start"]] = {"$gte": args.start_minimum, "$lte": args.start_maximum}
            if args.end_minimum is not None and args.end_maximum is not None:
                condition[fields["end"]] = {"$gte": args.end_minimum, "$lte": args.end_maximum}
            if args.variantType:
                condition[fields["variantType"]] = args.variantType
        
            # Only add non-empty conditions to or_conditions
            if condition:
                or_conditions.append(condition)
        
        # Create the query only if or_conditions is not empty
        query = {"$or": or_conditions} if or_conditions else {}

    # Connect to MongoDB collection
    advanced_required_args = ['database_auth_source', 'database_user', 'database_password']
    if any(getattr(args, arg)  != "" for arg in advanced_required_args):
        for arg in advanced_required_args:
            if not getattr(args, arg):
                print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                parsers[args.command].print_help()
                sys.exit(1)

    client = connect_to_mongodb(args)
    db = client[args.database]
    collection = db[args.collection]
    # Create a new dictionary with non-empty and non-default values
    filtered_query = {key: value for key, value in query.items() if value}
    for v in collection.find(filtered_query):
        pprint.pprint(v)

if __name__ == "__main__":
    beacon_query()
