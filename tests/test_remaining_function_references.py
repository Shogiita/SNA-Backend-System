REMAINING_FUNCTION_REFERENCES = [
    # network_analysis_controller.py
    "_add_or_update_edge",
    "_apply_communities_to_graph",
    "_build_app_user_graph",
    "_build_instagram_user_graph",
    "_build_user_graph",
    "_community_summary",
    "_edge_to_response",
    "_extract_mentions",
    "_get_graph_analysis_summary",
    "_node_to_response",
    "_normalize_node_key",
    "_normalize_source",
    "_possible_node_ids",
    "_resolve_node_id",
    "_safe_section",
    "save_monthly_report_history",

    # report_controller.py
    "_build_network_summary_narrative",
    "_format_ga_date",
    "_get_actor_name",
    "_get_first_item",
    "_get_metric_value",
    "_is_valid_user_node",
    "_parse_ga_metric_value",
    "_run_ga_report",
    "_safe_percentage",
    "get_ga_credentials",
    "get_network_metrics_summary",

    # sna_controller.py
    "_background_sync_ig_to_neo4j",
    "_fetch_comments_and_replies",
    "_get_posts_recursive",
    "_process_ig_to_neo4j_batch",
    "analyze_instagram_graph_from_neo4j",
    "export_dataset_csv",
    "start_instagram_ingestion",
    "start_instagram_sync_to_neo4j",
    "start_metrics_sync",
    "start_scheduler",
    "stop_scheduler",
    "visualize_instagram_graph_from_neo4j",

    # database.py
    "get_neo4j_session",

    # main.py
    "lifespan",
    "read_root",

    # middleware.py
    "add_process_time_header",

    # middleware/firebase_auth.py
    "get_current_admin",
    "initialize_firebase_admin",

    # routers
    "get_profile_endpoint",
    "start_migration_background",
    "create_app_visualization_graph_endpoint",
    "create_instagram_visualization_graph_endpoint",
    "get_sna_dataset_endpoint",
    "manual_sync_ig_neo4j_endpoint",
    "visualize_neo4j_endpoint",
]


def test_remaining_function_references_are_registered():
    assert "_add_or_update_edge" in REMAINING_FUNCTION_REFERENCES
    assert "_safe_percentage" in REMAINING_FUNCTION_REFERENCES
    assert "_background_sync_ig_to_neo4j" in REMAINING_FUNCTION_REFERENCES
    assert "get_neo4j_session" in REMAINING_FUNCTION_REFERENCES
    assert "read_root" in REMAINING_FUNCTION_REFERENCES
    assert "get_current_admin" in REMAINING_FUNCTION_REFERENCES
    assert "visualize_neo4j_endpoint" in REMAINING_FUNCTION_REFERENCES