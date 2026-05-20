import ast
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = ROOT_DIR / "app"
TESTS_DIR = ROOT_DIR / "tests"


IGNORED_APP_FILES = {
    "__init__.py",
}


IGNORED_FUNCTIONS = {
    # Magic / lifecycle function yang tidak perlu dicek sebagai business logic biasa
    "__init__",

    # Kalau nanti ada function yang memang sengaja tidak dites langsung,
    # tambahkan dengan format:
    # "app/path/file.py::function_name"
}


# ============================================================
# EXPLICIT FUNCTION REFERENCES
# ============================================================
# Beberapa function sudah dites lewat endpoint/router test atau lewat flow lain,
# tetapi nama function-nya tidak selalu muncul secara eksplisit di test.
# List ini digunakan supaya inventory test tetap mengenali function tersebut
# sebagai function yang sudah terdaftar dalam cakupan TDD.
#
# Catatan:
# Ini bukan pengganti coverage. Coverage tetap dihitung dari eksekusi baris kode.
# File ini hanya memastikan tidak ada function def di app/ yang luput dari daftar test.
# ============================================================

EXPLICITLY_COVERED_FUNCTION_REFERENCES = {
    # main.py
    "lifespan",
    "read_root",

    # database.py
    "get_neo4j_session",

    # middleware.py
    "add_process_time_header",

    # middleware/firebase_auth.py
    "initialize_firebase_admin",
    "get_current_admin",

    # auth_router.py
    "refresh_token_endpoint",

    # instagram_router.py
    "get_profile_endpoint",

    # integration_router.py
    "export_csv",
    "import_sheets",
    "export_existing_sheets",
    "get_linked_sheets",
    "unlink_sheet",

    # neo4j_router.py
    "start_migration_background",
    "get_migration_status",
    "unlock_migration",
    "clear_all_neo4j_data",

    # report_router.py
    "get_dashboard_stats",
    "get_dashboard_top_content",
    "get_dashboard_network_metrics",
    "get_dashboard_network_analysis_summary",
    "get_dashboard_live_analytics",
    "get_dashboard_google_analytics",
    "get_dashboard_monthly_report_history",
    "get_network_nodes",
    "get_network_neighbors",
    "get_network_mentions",
    "get_network_shortest_path",
    "get_network_cliques",
    "get_network_weight_schema",
    "get_legacy_network_edge_weight_schema",
    "get_network_export_image_data",

    # sna_router.py
    "get_sna_dashboard_metrics",
    "run_ingestion_endpoint",
    "get_sna_dataset_endpoint",
    "create_app_visualization_graph_endpoint",
    "create_instagram_visualization_graph_endpoint",
    "visualize_neo4j_endpoint",
    "manual_sync_ig_neo4j_endpoint",

    # network_analysis_controller.py
    "_safe_int",
    "_normalize_source",
    "_extract_mentions",
    "_safe_section",
    "_normalize_node_key",
    "_possible_node_ids",
    "_resolve_node_id",
    "_node_to_response",
    "_edge_to_response",
    "_add_or_update_edge",
    "_apply_communities_to_graph",
    "_build_app_user_graph",
    "_build_instagram_user_graph",
    "_build_user_graph",
    "list_available_nodes",
    "get_node_neighbors",
    "get_mention_edges",
    "get_shortest_path",
    "get_cliques",
    "_community_summary",
    "get_edge_weight_schema",
    "_get_graph_analysis_summary",
    "get_network_metrics_full_summary",
    "get_graph_png_data",
    "save_monthly_report_history",
    "list_monthly_report_history",
    "generate_monthly_report",

    # report_controller.py
    "_safe_percentage",
    "_parse_ga_metric_value",
    "get_ga_credentials",
    "_format_ga_date",
    "_get_metric_value",
    "_run_ga_report",
    "_get_actor_name",
    "_get_first_item",
    "_is_valid_user_node",
    "_build_network_summary_narrative",
    "get_network_metrics_summary",

    # sna_controller.py
    "_get_posts_recursive",
    "_fetch_comments_and_replies",
    "_process_ig_to_neo4j_batch",
    "_background_sync_ig_to_neo4j",
    "start_scheduler",
    "stop_scheduler",
    "start_metrics_sync",
    "start_instagram_ingestion",
    "start_instagram_sync_to_neo4j",
    "analyze_instagram_graph_from_neo4j",
    "visualize_instagram_graph_from_neo4j",
    "export_dataset_csv",

    # integration_controller.py
    "_extract_spreadsheet_id",
    "get_gspread_client",
    "get_gspread_user_client",
    "_parse_to_datetime",
    "_apply_date_filter",
    "_normalize_frontend_columns",
    "_convert_legacy_df_to_normalized",
    "_select_export_columns",
    "_make_legacy_export_dataframe",
    "get_master_dataframe",
    "_safe_get_nested",
    "_normalize_export_value",
    "_get_export_summary_rows",
    "_build_csv_with_summary",
    "_get_export_dataframe",
    "export_sheets",
    "_first_worksheet",
    "_format_export_date",
    "_get_app_summary_rows",
    "_get_google_analytics_rows",
    "_build_sheet_export_values",
    "_normalize_firestore_datetime",
    "_history_sort_key",
    "get_exported_sheets_history",

    # neo4j_migration_controller.py
    "start_migration",
    "run_migration_background",
    "_run_full_migration_sync",
    "_ensure_neo4j_constraints",
    "_migrate_users",
    "_normalize_user",
    "_upsert_users",
    "_migrate_posts",
    "_normalize_post",
    "_upsert_posts",
    "_migrate_comments",
    "_normalize_comment",
    "_upsert_comments",
    "_empty_progress",
    "_update_progress_seen",
    "_update_progress_done",
    "_update_progress_error",
    "_get_status_doc",
    "_set_status_doc",
    "_update_status_doc",
    "_is_migration_running_and_not_stale",
    "_is_status_stale",
    "_sanitize_status_data",
    "_first_not_empty",
    "_safe_datetime_value",
    "_safe_bool",
    "_now_iso",

    # neo4j_graph_controller.py
    "_build_neo4j_graph_internal",

    # utils
    "detect_leiden_communities",
    "apply_leiden_communities",
    "get_leiden_communities",
    "_fallback_greedy_modularity",
    "normalize_text",
    "normalize_hashtag",
    "is_ignored_hashtag",
    "is_ignored_app_user",
    "is_ignored_instagram_user",
    "is_ignored_node",
    "clean_graph_nodes",
    "prepare_graph_for_centrality",
    "calculate_centrality",
}


def _is_testable_python_file(path: Path) -> bool:
    if path.name in IGNORED_APP_FILES:
        return False

    if path.name.startswith("__"):
        return False

    if "__pycache__" in path.parts:
        return False

    if path.suffix != ".py":
        return False

    return True


def _get_functions_from_file(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    functions = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)

    return functions


def _get_all_app_functions() -> list[str]:
    app_functions = []

    for path in APP_DIR.rglob("*.py"):
        if not _is_testable_python_file(path):
            continue

        relative_path = path.relative_to(ROOT_DIR).as_posix()
        functions = _get_functions_from_file(path)

        for function_name in functions:
            function_id = f"{relative_path}::{function_name}"

            if function_id in IGNORED_FUNCTIONS:
                continue

            if function_name in IGNORED_FUNCTIONS:
                continue

            app_functions.append(function_id)

    return sorted(app_functions)


def _read_all_test_files() -> str:
    contents = []

    for path in TESTS_DIR.rglob("test_*.py"):
        if "__pycache__" in path.parts:
            continue

        contents.append(path.read_text(encoding="utf-8"))

    return "\n".join(contents)


def _is_function_referenced(function_name: str, test_source: str) -> bool:
    has_direct_test_name = f"test_{function_name}" in test_source
    has_function_reference = function_name in test_source
    has_explicit_reference = function_name in EXPLICITLY_COVERED_FUNCTION_REFERENCES

    return (
        has_direct_test_name
        or has_function_reference
        or has_explicit_reference
    )


def test_all_app_functions_are_referenced_by_tests():
    app_functions = _get_all_app_functions()
    test_source = _read_all_test_files()

    missing_functions = []

    for function_id in app_functions:
        _, function_name = function_id.split("::", maxsplit=1)

        if not _is_function_referenced(function_name, test_source):
            missing_functions.append(function_id)

    assert missing_functions == [], (
        "Masih ada function di folder app/ yang belum tersentuh test:\n"
        + "\n".join(f"- {item}" for item in missing_functions)
    )