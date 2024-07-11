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
    
    This script provides a command-line interface for querying different collections in a Beacon Database using various sub-commands for sequence, range, gene ID, or bracket criteria.
    
    Example Usage:
    
    1. Query genomicVariations collection by sequence:
        beacon_search sequence -d database_name -c collection_name -rn reference_name -s start -ab alternate_bases
    
    2. Query genomicVariations collection by range:
        beacon_search range -d database_name -c collection_name -rn reference_name -s start -e end -v variant_type
    
    3. Query genomicVariations collection by gene ID:
        beacon_search gene -d database_name -c collection_name -g gene_id -vmin variant_min_length -vmax variant_max_length
    
    4. Query genomicVariations collection by bracket:
        beacon_search bracket -d database_name -c collection_name -rn reference_name -smin start_minimum -smax start_maximum -emin end_minimum -emax end_maximum -v variant_type
    
    5. Query analyses collection:
        beacon_search analyses -d database_name -c collection_name -al aligner -ad analysis_date -bi biosample_id -id identification -ii individual_id -pn pipeline_name -pr pipeline_ref -ri run_id -vc variant_caller
    
    6. Query biosamples collection:
        beacon_search biosamples -d database_name -c collection_name -bs biosample_status -cd collection_date -cm collection_moment -id identification -op obtention_procedure -so sample_origin_type
    
    7. Query cohorts collection:
        beacon_search cohorts -d database_name -c collection_name -ct cohort_data_types -cd cohort_design -cz cohort_size -t cohort_type -id identification -g genders -n name
    
    8. Query datasets collection:
        beacon_search datasets -d database_name -c collection_name -o ontology -om ontology_modifiers -id identification -n name
    
    9. Query individuals collection:
        beacon_search individuals -d database_name -c collection_name -g age_group -do disease_ontology -f family_history -se severity -st stage -e ethnicity -go geographic_origin -id identification -as assay_code -s sex
    
    10. Query runs collection:
        beacon_search runs -d database_name -c collection_name -id identification -ii individual_id -ll library_layout -ls library_selection -s library_source -st library_strategy -p platform -pm platform_model -r run_date
    
    11. Query for cnv:

        beacon_search cnv  -d database_name -c collection_name -id identification -ii individual_id
    """
    
    parser = argparse.ArgumentParser(description="Query Beacon Database")
    subparsers = parser.add_subparsers(dest="command")
    parsers = {}
    # subparsers.required = True

    # Sub-parser for command "Beacon Sequence queries"
    parser_sequence = subparsers.add_parser("sequence", help="Connect to MongoDB and perform sequence-based queries to the genomicVariations collection")
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
    
    # Sub-parser for command "Beacon Range queries"
    parser_range = subparsers.add_parser("range", help="Connect to MongoDB and perform range-based queries to the genomicVariations collection")
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
    
    # Sub-parser for command "Beacon GeneId queries"
    parser_gene = subparsers.add_parser("gene", help="Connect to MongoDB and perform geneID-based queries to the genomicVariations collection")
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
    
    # Sub-parser for command "Beacon Bracket queries"

    # Sub-parser for command "Beacon Bracket queries"
    parser_bracket = subparsers.add_parser("bracket", help="Connect to MongoDB and perform bracket-based queries to the genomicVariations collection")
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
    
    
    # Sub-parser for command "Beacon analyses queries"

    # Sub-parser for command "Beacon analyses queries"
    parser_analyses = subparsers.add_parser("analyses", help="Connect to MongoDB and query the analyses collection")
    common_arguments(parser_analyses)
    parsers["analyses"] = parser_analyses
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_analyses.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-al", "--aligner", type=str, default="", dest="aligner", help="Aligner")
    optional_query_group.add_argument("-ad", "--analysisDate", type=str, default="", dest="analysisDate", help="Analysis Date")
    optional_query_group.add_argument("-bi", "--biosampleId", type=str, default="", dest="biosampleId", help="Biosample ID")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-ii", "--individualId", type=str, default="", dest="individualId", help="Individual ID")
    optional_query_group.add_argument("-pn", "--pipelineName", type=str, default="", dest="pipelineName", help="Pipeline Name")
    optional_query_group.add_argument("-pr", "--pipelineRef", type=str, default="", dest="pipelineRef", help="Pipeline Reference")
    optional_query_group.add_argument("-ri", "--runId", type=str, default="", dest="runId", help="Run ID")
    optional_query_group.add_argument("-vc", "--variantCaller", type=str, default="", dest="variantCaller", help="Variant Caller")
    


    # Sub-parser for command "Beacon Biosample queries"
    parser_biosamples = subparsers.add_parser("biosamples", help="Connect to MongoDB and query the biosample collection")
    common_arguments(parser_biosamples)
    parsers["biosamples"] = parser_biosamples
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_biosamples.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-bs", "--biosampleStatus", type=str, default="", dest="biosampleStatus", help="Biosample Status")
    optional_query_group.add_argument("-cd", "--collectionDate", type=str, default="", dest="collectionDate", help="Collection Date")
    optional_query_group.add_argument("-cm", "--collectionMoment", type=str, default="", dest="collectionMoment", help="Mollection Moment")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-dm", "--diagnosticMarkers", type=str, default="", dest="diagnosticMarkers", help="Diagnostic Markers")
    optional_query_group.add_argument("-hd", "--histologicalDiagnosis", type=str, default="", dest="histologicalDiagnosis", help="Histological Diagnosis")
    optional_query_group.add_argument("-op", "--obtentionProcedure", type=str, default="", dest="obtentionProcedure", help="Obtention Procedure")
    optional_query_group.add_argument("-ps", "--pathologicalStage", type=str, default="", dest="pathologicalStage", help="Pathological Stage")
    optional_query_group.add_argument("-pf", "--pathologicalTnmFinding", type=str, default="", dest="pathologicalTnmFinding", help="Pathological Tnm Finding")
    optional_query_group.add_argument("-ft", "--featureType", type=str, default="", dest="featureType", help="Feature Type")
    optional_query_group.add_argument("-s", "--severity", type=str, default="", dest="severity", help="s")
    optional_query_group.add_argument("-sd", "--sampleOriginDetail", type=str, default="", dest="sampleOriginDetail", help="Sample Origin Detail")
    optional_query_group.add_argument("-so", "--sampleOriginType", type=str, default="", dest="sampleOriginType", help="Sample Origin Type")
    optional_query_group.add_argument("-sp", "--sampleProcessing", type=str, default="", dest="sampleProcessing", help="Sample Processing")
    optional_query_group.add_argument("-ss", "--sampleStorage", type=str, default="", dest="sampleStorage", help="Sample Storage")
    optional_query_group.add_argument("-tg", "--tumorGrade", type=str, default="", dest="tumorGrade", help="Tumor Grade")
    optional_query_group.add_argument("-tp", "--tumorProgression", type=str, default="", dest="tumorProgression", help="Tumor Progression")

    # Sub-parser for command "Beacon cohorts queries"
    parser_cohorts = subparsers.add_parser("cohorts", help="Connect to MongoDB and query the cohorts collection")
    common_arguments(parser_cohorts)
    parsers["cohorts"] = parser_cohorts
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_cohorts.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-ct", "--cohortDataTypes", type=str, default="", dest="cohortDataTypes", help="Cohort Data Types")
    optional_query_group.add_argument("-cd", "--cohortDesign", type=str, default="", dest="cohortDesign", help="Cohort Design")
    optional_query_group.add_argument("-cz", "--cohortSize", type=int, default=None, dest="cohortSize", help="Cohort Size")
    optional_query_group.add_argument("-t", "--cohortType", type=str, default="", dest="cohortType", help="Cohort Type")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-g", "--genders", type=str, default="", dest="genders", help="Genders")
    optional_query_group.add_argument("-n", "--name", type=str, default="", dest="name", help="Name")
    
    # Sub-parser for command "Beacon datasets Queries"
    parser_datasets = subparsers.add_parser("datasets", help="Connect to MongoDB and query the datasets collection")
    common_arguments(parser_datasets)
    parsers["datasets"] = parser_datasets
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_datasets.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-du", "--dataUseConditions", type=str, default="", dest="dataUseConditions", help="Data Use Conditions")
    optional_query_group.add_argument("-om", "--ontologyModifiers", type=str, default="", dest="ontologyModifiers", help="Data Use Conditions Modifiers")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-n", "--name", type=str, default="", dest="name", help="Name")
    
    # Sub-parser for command "Beacon individuals Queries"
    parser_individuals = subparsers.add_parser("individuals", help="Connect to MongoDB and query the individuals collection")
    common_arguments(parser_individuals)
    parsers["individuals"] = parser_individuals
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_individuals.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-g", "--ageGroup", type=str, default="", dest="ageGroup", help="Age Group")
    optional_query_group.add_argument("-do", "--diseaseCode", type=str, default="", dest="diseaseCode", help="Disease Code")
    optional_query_group.add_argument("-f", "--familyHistory", type=str, default="", dest="familyHistory", help="Family History")
    optional_query_group.add_argument("-se", "--severity", type=str, default="", dest="severity", help="Severity")
    optional_query_group.add_argument("-st", "--stage", type=str, default="", dest="stage", help="Stage")
    optional_query_group.add_argument("-e", "--ethnicity", type=str, default="", dest="ethnicity", help="Ethnicity")
    optional_query_group.add_argument("-go", "--geographicOrigin", type=str, default="", dest="geographicOrigin", help="Geographic Origin")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-as", "--assayCode", type=str, default="", dest="assayCode", help="Measures Ontology")
    optional_query_group.add_argument("-s", "--sex", type=str, default="", dest="sex", help="sex")
    
    # Sub-parser for command "Beacon runs Queries"
    parser_runs = subparsers.add_parser("runs", help="Connect to MongoDB and query the runs collection")
    common_arguments(parser_runs)
    parsers["runs"] = parser_runs
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_runs.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-id", "--identification", type=str, default="", dest="identification", help="Identification")
    optional_query_group.add_argument("-ii", "--individualId", type=str, default="", dest="individualId", help="Individual Id")
    optional_query_group.add_argument("-ll", "--libraryLayout", type=str, default="", dest="libraryLayout", help="Library Layout")
    optional_query_group.add_argument("-ls", "--librarySelection", type=str, default="", dest="librarySelection", help="Library Selection")
    optional_query_group.add_argument("-s", "--librarySource", type=str, default="", dest="librarySource", help="Library Source")
    optional_query_group.add_argument("-st", "--libraryStrategy", type=str, default="", dest="libraryStrategy", help="Library Strategy")
    optional_query_group.add_argument("-p", "--platform", type=str, default="", dest="platform", help="platform")
    optional_query_group.add_argument("-pm", "--platformModel", type=str, default="", dest="platformModel", help="Platform Model")
    optional_query_group.add_argument("-r", "--runDate", type=str, default="", dest="runDate", help="Run Date")
    
    # Sub-parser for command "Beacon cnv Queries"
    parser_cnv = subparsers.add_parser("cnv", help="Connect to MongoDB and query the copy number variants (cnv) collection")
    common_arguments(parser_cnv)
    parsers["cnv"] = parser_cnv
    
    
    # Optional Search Query Parameters
    
    optional_query_group = parser_cnv.add_argument_group("Optional Database Query Arguments")
    optional_query_group.add_argument("-vi", "--variantInternalId", type=str, default="", dest="variantInternalId", help="VariantInternal Id")
    optional_query_group.add_argument("-ai", "--analysisId", type=str, default="", dest="analysisId", help="Analysis Id")
    optional_query_group.add_argument("-ii", "--individualId", type=str, default="", dest="individualId", help="Individual Id")
    optional_query_group.add_argument("-s", "--start", type=int, default=None, dest="start", help="start")
    optional_query_group.add_argument("-e", "--end", type=int, default=None, dest="end", help="end")
    optional_query_group.add_argument("-ch", "--chromosome", type=str, default="", dest="chromosome", help="Chromosome")
    optional_query_group.add_argument("-si", "--variantStateId", type=str, default="", dest="variantStateId", help="Variant State Id")
    optional_query_group.add_argument("-vs", "--variantState", type=str, default="", dest="variantState", help="Variant State")
    optional_query_group.add_argument("-sd", "--sequenceId", type=str, default="", dest="sequenceId", help="Sequence Id")
    
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
        
        query = {
            "variation.location.sequence_id": args.referenceName,
            "variation.location.interval.start.value": args.start,
            "variation.alternateBases": args.alternateBases,
            "variation.referenceBases": args.referenceBases,
            "caseLevelData.biosampleId": {"$in": args.collectionIds} if args.collectionIds else None
        }

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
        
        
        query = {
            "variation.location.sequence_id": args.referenceName,
            "variation.location.interval.start.value": args.start,
            "variation.location.interval.end.value": args.end,
            "variation.variantType": args.variantType,
            "variation.alternateBases": args.alternateBases,
            "molecularAttributes.aminoacidChanges": args.aminoacidChange,
            "variation.location.interval.start.value": {"$gte": args.variantMinLength} if args.variantMinLength is not None else None,
            "variation.location.interval.end.value": {"$lte": args.variantMaxLength} if args.variantMaxLength is not None else None
        }


    # query gene_id_queries
    elif args.command == "gene":
        required_args = ['database', 'collection', 'database_host', 'database_port','geneId']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        query = {
            "molecularAttributes.geneIds": args.geneId,
            "variation.variantType": args.variantType,
            "variation.alternateBases": args.alternateBases,
            "molecularAttributes.aminoacidChanges": args.aminoacidChange,
            "variation.location.interval.start.value": {"$gte": args.variantMinLength} if args.variantMinLength is not None else None,
            "variation.location.interval.end.value": {"$lte": args.variantMaxLength} if args.variantMaxLength is not None else None
        }


    # query bracket_queries
    elif args.command == "bracket":
        required_args = ['database', 'collection', 'database_host', 'database_port','start_minimum','start_maximum','end_minimum','end_maximum']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        query = {
            "variation.location.sequence_id": args.referenceName,
            "variation.location.interval.start.value": {"$gte": args.start_minimum, "$lte": args.start_maximum} if args.start_minimum is not None and args.start_maximum is not None else None,
            "variation.location.interval.end.value": {"$gte": args.end_minimum, "$lte": args.end_maximum} if args.end_minimum is not None and args.end_maximum is not None else None,
            "variation.variantType": args.variantType
        }
    # query analyses collection
    elif args.command == "analyses":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        query = {
            "aligner": args.aligner,
            "analysisDate": args.analysisDate,
            "biosampleId": args.biosampleId,
            "id": args.identification,
            "individualId": args.individualId,
            "pipelineName": args.pipelineName,
            "pipelineRef": args.pipelineRef,
            "runId": args.runId,
            "variantCaller": args.variantCaller
        }
        
        
    # query biosample collection
    elif args.command == "biosamples":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        query = {
            "biosampleStatus.label": args.biosampleStatus,
            "collectionDate": args.collectionDate,
            "collectionMoment": args.collectionMoment,
            "id": args.identification,
            "obtentionProcedure.procedureCode.label": args.obtentionProcedure,
            "sampleOriginType.label": args.sampleOriginType,
            "histologicalDiagnosis.label": args.histologicalDiagnosis,
            "pathologicalStage.label": args.pathologicalStage,
            "pathologicalTnmFinding.label": args.pathologicalTnmFinding,
            "phenotypicFeatures.featureType.label": args.featureType,
            "phenotypicFeatures.severity.label": args.severity,
            "sampleOriginDetail.label": args.sampleOriginDetail,
            "sampleProcessing.label": args.sampleProcessing,
            "sampleStorage.label": args.sampleStorage,
            "tumorGrade.label": args.tumorGrade,
            "tumorProgression.label": args.tumorProgression
        }

    # query cohorts collection
    elif args.command == "cohorts":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)

        query = {
            "cohortDataTypes.label": args.cohortDataTypes,
            "cohortDesign.label": args.cohortDesign,
            "cohortSize": args.cohortSize,
            "cohortType": args.cohortType,
            "id": args.identification,
            "inclusionCriteria.genders.label": args.genders,
            "name": args.name
        }

    # query datasets collection
    elif args.command == "datasets":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)

        query = {
            "dataUseConditions.duoDataUse.label": args.dataUseConditions,
            "dataUseConditions.duoDataUse.modifiers.label": args.ontologyModifiers,
            "id": args.identification,
            "name": args.name
        }

    # query individuals collection
    elif args.command == "individuals":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)

        query = {
            "diseases.ageOfOnset.ageGroup.label": args.ageGroup,
            "diseases.diseaseCode.label": args.diseaseCode,
            "diseases.familyHistory": args.familyHistory,
            "diseases.severity": args.severity,
            "diseases.stage": args.stage,
            "ethnicity.label": args.ethnicity,
            "geographicOrigin.label": args.geographicOrigin,
            "id": args.identification,
            "measures.assayCode.label": args.assayCode,
            "sex.label": args.sex
        }
    
    
    # query individuals collection
    elif args.command == "runs":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        query = {
            "id": args.identification,
            "individualId": args.individualId,
            "libraryLayout": args.libraryLayout,
            "librarySelection": args.librarySelection,
            "librarySource.label": args.librarySource,
            "libraryStrategy": args.libraryStrategy,
            "platform": args.platform,
            "platformModel.label": args.platformModel,
            "runDate": args.runDate
        }
    
    # query individuals collection
    elif args.command == "cnv":
        required_args = ['database', 'collection', 'database_host', 'database_port']
        if any(getattr(args, arg)  != "" for arg in required_args):
            for arg in required_args:
                if not getattr(args, arg):
                    print(f"Missing value -> {arg}. Use -h or --help for usage details.")
                    parsers[args.command].print_help()
                    sys.exit(1)
        
        query = {
            "variantInternalId": args.variantInternalId,
            "analysisId": args.analysisId,
            "individualId": args.individualId,
            "definitions.Location.start": args.start,
            "definitions.Location.end": args.end,
            "definitions.Location.chromosome": args.chromosome,
            "variantState.id": args.variantStateId,
            "variantState.label": args.variantState,
            "definitions.Location.sequenceId": args.sequenceId
        }

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
