#!/usr/bin/env python3
"""
Script to automatically configure Superset with:
- Connection to ClickHouse
- Datasets from dbt transformed tables
- Dashboard with statistical charts

Usage:
  python3 setup_superset_dataflix.py [--clean]
"""

import subprocess
import time
import sys
import requests
import json
import argparse

# Configuration
SUPERSET_URL = "http://localhost:8088"
CLICKHOUSE_HOST = "clickhouse-server"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse123"
CLICKHOUSE_DB = "analytics"

# Output colors
GREEN = '\033[92m'
BLUE = '\033[94m'
YELLOW = '\033[93m'
RED = '\033[91m'
END = '\033[0m'


def log_info(msg):
    print(f"{BLUE}[INFO]{END} {msg}")


def log_success(msg):
    print(f"{GREEN}[SUCCESS]{END} {msg}")


def log_warning(msg):
    print(f"{YELLOW}[WARNING]{END} {msg}")


def log_error(msg):
    print(f"{RED}[ERROR]{END} {msg}")


def wait_for_superset(max_retries=60):
    """Waits for Superset to become available"""
    log_info("Waiting for Superset to become available...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if response.status_code == 200:
                log_success("Superset is available!")
                return True
        except:
            pass
        if i < max_retries - 1:
            time.sleep(2)
    log_error("Superset did not become available in time")
    return False


def run_python_in_container(script):
    """Executes Python script inside Superset container"""
    cmd = ['docker', 'exec', '-i', 'superset-dataflix', 'python']
    result = subprocess.run(cmd, input=script, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr


def clean_superset():
    """Removes previously created dashboard, charts, and datasets"""
    log_info("Cleaning old Superset configurations...")
    
    script = '''
import sys
import os
sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    from superset.extensions import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable

    # 1. Remove Dashboard
    dashboards = db.session.query(Dashboard).filter(Dashboard.slug.in_(["dataflix-analytics", "dataflix-analytics2"])).all()
    for d in dashboards:
        print(f"Removing dashboard: {d.dashboard_title}")
        db.session.delete(d)
    
    # 2. Remove Charts
    chart_names = [
        "Movies by Genre", 
        "Average Rating by Movie", 
        "Total Movies Watched", 
        "Users by State", 
        "User-Movie Interaction Matrix", 
        "Aggregated Ratings"
    ]
    charts = db.session.query(Slice).filter(Slice.slice_name.in_(chart_names)).all()
    for c in charts:
        print(f"Removing chart: {c.slice_name}")
        db.session.delete(c)

    # 3. Remove Datasets
    table_names = ["mart_users", "mart_movies", "mart_user_movie_matrix", "mart_ratings_aggregated"]
    tables = db.session.query(SqlaTable).filter(SqlaTable.table_name.in_(table_names)).all()
    for t in tables:
        print(f"Removing dataset: {t.table_name}")
        db.session.delete(t)
        
    db.session.commit()
    print("Cleanup completed.")
'''
    success, stdout, stderr = run_python_in_container(script)
    if success:
        log_success("Cleanup performed successfully!")
        print(stdout)
    else:
        log_error(f"Cleanup error: {stderr}")


def setup_database():
    """Creates ClickHouse connection in Superset"""
    log_info("Configuring ClickHouse connection...")

    script = f'''
import sys
import os
sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database

    # Check if database already exists
    existing = db.session.query(Database).filter_by(database_name="ClickHouse Analytics").first()
    if existing:
        print(f"Database already exists with ID: {{existing.id}}")
    else:
        # Create new database
        new_db = Database(
            database_name="ClickHouse Analytics",
            sqlalchemy_uri="clickhousedb+connect://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}",
            expose_in_sqllab=True,
            allow_ctas=False,
            allow_cvas=False,
            allow_dml=False,
            allow_run_async=True
        )
        db.session.add(new_db)
        db.session.commit()
        print(f"Database created with ID: {{new_db.id}}")
'''

    success, stdout, stderr = run_python_in_container(script)

    if success or "already exists" in stdout:
        log_success("ClickHouse connection configured!")
        return True
    else:
        log_warning(f"Configuration problem: {stderr}")
        return True


def setup_datasets():
    """Creates datasets from ClickHouse tables and defines metrics"""
    log_info("Configuring datasets and metrics...")

    # Definition of tables and their metrics
    tables_config = {
        "mart_users": [
            {"name": "user_count", "expression": "COUNT(*)", "verbose_name": "Total Users", "metric_type": "count"},
            {"name": "avg_movies_watched", "expression": "AVG(total_movies_watched)", "verbose_name": "Average Movies Watched", "metric_type": "avg"}
        ],
        "mart_movies": [
            {"name": "genre_count", "expression": "COUNT(*)", "verbose_name": "Genre Count", "metric_type": "count"},
            {"name": "sum_total_watches", "expression": "SUM(total_watches)", "verbose_name": "Total Views", "metric_type": "sum"},
            {"name": "avg_user_rating", "expression": "AVG(average_user_rating)", "verbose_name": "Average Rating", "metric_type": "avg"}
        ],
        "mart_user_movie_matrix": [
            {"name": "avg_interaction", "expression": "AVG(interaction_score)", "verbose_name": "Average Interaction", "metric_type": "avg"}
        ],
        "mart_ratings_aggregated": [
            {"name": "sum_total_ratings", "expression": "SUM(total_ratings)", "verbose_name": "Total Ratings", "metric_type": "sum"},
            {"name": "avg_rating_agg", "expression": "AVG(average_rating)", "verbose_name": "Average Ratings", "metric_type": "avg"},
            {"name": "avg_like_percentage", "expression": "AVG(like_percentage)", "verbose_name": "Like Percentage", "metric_type": "avg"}
        ]
    }

    for table, metrics in tables_config.items():
        metrics_json = json.dumps(metrics)
        script = f'''
import sys
import os
import traceback
import json
sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    try:
        from superset.extensions import db
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable, SqlMetric

        # Get database
        database = db.session.query(Database).filter_by(database_name="ClickHouse Analytics").first()
        if not database:
            print("Database not found")
            sys.exit(1)

        # Check/Create Dataset
        table_obj = db.session.query(SqlaTable).filter_by(table_name="{table}", database_id=database.id).first()
        if not table_obj:
            table_obj = SqlaTable(
                table_name="{table}",
                database_id=database.id,
                schema="{CLICKHOUSE_DB}"
            )
            db.session.add(table_obj)
            db.session.flush()
            print(f"Dataset {{table_obj.table_name}} created with ID: {{table_obj.id}}")
        else:
            print(f"Dataset {{table_obj.table_name}} already exists with ID: {{table_obj.id}}")

        # Fetch metadata (columns)
        try:
            print(f"Fetching metadata for {table}...")
            table_obj.fetch_metadata()
        except Exception as e:
            print(f"Error fetching metadata: {{e}}")

        # Add Metrics
        metrics_config = json.loads('{metrics_json}')
        
        for m_conf in metrics_config:
            metric_name = m_conf['name']
            
            # Check if metric already exists
            metric = db.session.query(SqlMetric).filter_by(
                table_id=table_obj.id,
                metric_name=metric_name
            ).first()
            
            if not metric:
                metric = SqlMetric(
                    metric_name=metric_name,
                    expression=m_conf['expression'],
                    verbose_name=m_conf['verbose_name'],
                    metric_type=m_conf['metric_type'],
                    table_id=table_obj.id
                )
                db.session.add(metric)
                print(f"Metric created: {{metric_name}}")
            else:
                metric.expression = m_conf['expression']
                metric.verbose_name = m_conf['verbose_name']
                print(f"Metric updated: {{metric_name}}")
        
        db.session.commit()

    except Exception as e:
        print("EXCEPTION_OCCURRED")
        traceback.print_exc()
        sys.exit(1)
'''
        success, stdout, stderr = run_python_in_container(script)
        if success:
            log_success(f"Dataset '{table}' configured with metrics!")
        else:
            log_warning(f"Dataset '{table}': {stdout} {stderr}")


def setup_dashboard():
    """Creates dashboard"""
    log_info("Configuring dashboard...")

    json_metadata = json.dumps({
        "chart_configuration": {
            "1": {"id": 1, "crossFilters": {"scope": "global", "chartsInScope": [2, 3, 4, 5, 6]}},
            "6": {"id": 6, "crossFilters": {"scope": "global", "chartsInScope": [1, 2, 3, 4, 5]}}
        },
        "global_chart_configuration": {
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": [1, 2, 3, 4, 5, 6]
        },
        "color_scheme": "",
        "refresh_frequency": 0,
        "expanded_slices": {},
        "label_colors": {},
        "timed_refresh_immune_slices": [],
        "cross_filters_enabled": True,
        "default_filters": "{}",
        "shared_label_colors": {},
        "color_scheme_domain": []
    })
    
    json_metadata_escaped = json_metadata.replace("'", "\\'")

    script = f'''
import sys
import os
sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    from superset.extensions import db
    from superset.models.dashboard import Dashboard
    from flask_appbuilder.security.sqla.models import User

    # Check if dashboard already exists
    existing = db.session.query(Dashboard).filter_by(slug="dataflix-analytics").first()
    if existing:
        print(f"Dashboard already exists with ID: {{existing.id}}")
        existing.json_metadata = '{json_metadata_escaped}'
        db.session.commit()
    else:
        # Get admin user
        admin = db.session.query(User).filter_by(username="admin").first()

        # Create dashboard
        new_dash = Dashboard(
            dashboard_title="Dataflix Analytics",
            slug="dataflix-analytics",
            published=True,
            json_metadata='{json_metadata_escaped}'
        )
        if admin:
            new_dash.owners = [admin]
        db.session.add(new_dash)
        db.session.commit()
        print(f"Dashboard created with ID: {{new_dash.id}}")
'''

    success, stdout, stderr = run_python_in_container(script)

    if success or "already exists" in stdout:
        log_success("Dashboard configured!")
        return True
    else:
        log_warning(f"Dashboard problem: {stderr[:200]}")
        return True


def setup_charts():
    """Creates charts using defined metrics"""
    log_info("Configuring charts...")

    # NOTE: Now we reference metrics by name defined in setup_datasets
    charts = [
        ("Movies by Genre", "pie", "mart_movies", 
         '{"viz_type": "pie", "groupby": ["genre"], "metric": "genre_count", "adhoc_filters": [], "row_limit": 50, "sort_by_metric": true, "color_scheme": "d3Category10", "show_legend": true, "show_labels": true, "donut": true, "labels_outside": true}'),
         
        ("Average Rating by Movie", "dist_bar", "mart_movies", 
         '{"viz_type": "dist_bar", "groupby": ["title"], "metrics": ["avg_user_rating"], "adhoc_filters": [], "row_limit": 20, "color_scheme": "d3Category10", "show_legend": false, "show_bar_value": true, "order_desc": true, "rich_tooltip": true, "y_axis_format": ".1f"}'),
         
        ("Total Movies Watched", "big_number_total", "mart_movies", 
         '{"viz_type": "big_number_total", "metric": "sum_total_watches", "adhoc_filters": [], "subheader": "Total Views", "y_axis_format": ",d"}'),
         
        ("Users by State", "dist_bar", "mart_users", 
         '{"viz_type": "dist_bar", "groupby": ["state"], "metrics": ["user_count"], "adhoc_filters": [], "row_limit": 50, "color_scheme": "d3Category10", "show_legend": true, "show_bar_value": true, "order_desc": true, "y_axis_format": ",d"}'),
         
        ("User-Movie Interaction Matrix", "heatmap", "mart_user_movie_matrix", 
         '{"viz_type": "heatmap", "all_columns_x": "username", "all_columns_y": "title", "metric": "avg_interaction", "adhoc_filters": [], "row_limit": 100, "linear_color_scheme": "superset_seq_1", "canvas_image_rendering": "pixelated", "normalize_across": "heatmap", "y_axis_format": ".1f", "show_legend": true, "show_perc": true}'),
         
        ("Aggregated Ratings", "table", "mart_ratings_aggregated", 
         '{"viz_type": "table", "query_mode": "aggregate", "groupby": ["title"], "metrics": ["sum_total_ratings", "avg_rating_agg", "avg_like_percentage"], "adhoc_filters": [], "row_limit": 100, "order_desc": true, "show_cell_bars": true, "color_pn": true}')
    ]

    for chart_name, viz_type, table_name, params in charts:
        params_escaped = params.replace("'", "\\'")

        script = f'''
import sys
import os
import traceback
sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    try:
        from superset.extensions import db
        from superset.models.slice import Slice
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from flask_appbuilder.security.sqla.models import User

        # Check if chart already exists
        existing = db.session.query(Slice).filter_by(slice_name="{chart_name}").first()
        if existing:
            print(f"Chart already exists with ID: {{existing.id}} - Updating params")
            existing.params = '{params_escaped}'
            db.session.commit()
        else:
            # Get dataset
            dataset = db.session.query(SqlaTable).filter_by(table_name="{table_name}").first()
            if not dataset:
                print("Dataset not found")
                sys.exit(1)

            # Get admin user
            admin = db.session.query(User).filter_by(username="admin").first()

            # Get dashboard
            dashboard = db.session.query(Dashboard).filter_by(slug="dataflix-analytics").first()

            # Create chart
            new_chart = Slice(
                slice_name="{chart_name}",
                viz_type="{viz_type}",
                datasource_type="table",
                datasource_id=dataset.id,
                params='{params_escaped}'
            )
            if admin:
                new_chart.owners = [admin]
            
            db.session.add(new_chart)
            db.session.flush()
            
            if dashboard:
                new_chart.dashboards.append(dashboard)
                
            db.session.commit()
            print(f"Chart created with ID: {{new_chart.id}}")
            
    except Exception as e:
        print("EXCEPTION_OCCURRED")
        traceback.print_exc()
        sys.exit(1)
'''

        success, stdout, stderr = run_python_in_container(script)

        if success or "already exists" in stdout:
            log_success(f"Chart '{chart_name}' configured!")
        else:
            log_warning(f"Chart '{chart_name}': {stdout} {stderr}")


def update_dashboard_layout():
    """Updates dashboard layout to show charts"""
    log_info("Updating dashboard layout...")
    
    script = '''
import sys
import os
import json
import uuid
import traceback

sys.path.insert(0, '/app')
os.chdir('/app')

from superset.app import create_app
app = create_app()

with app.app_context():
    try:
        from superset.extensions import db
        from superset.models.dashboard import Dashboard
        
        dashboard = db.session.query(Dashboard).filter_by(slug="dataflix-analytics").first()
        if not dashboard:
            print("Dashboard not found")
            sys.exit(1)
            
        charts = dashboard.slices
        print(f"Found {len(charts)} charts for dashboard")
        
        if not charts:
            print("No charts for layout")
            sys.exit(0)
            
        chart_map = {c.slice_name: c for c in charts}
        
        def get_chart_node(name, width, height=50):
            chart = chart_map.get(name)
            if not chart:
                print(f"Warning: Chart '{name}' not found")
                return None
                
            return {
                "children": [],
                "id": f"CHART-{uuid.uuid4().hex[:8]}",
                "meta": {
                    "chartId": chart.id,
                    "height": height,
                    "sliceName": chart.slice_name,
                    "uuid": str(uuid.uuid4()),
                    "width": width
                },
                "type": "CHART",
                "parents": [] 
            }

        # Layout: 2 rows
        row1_charts = [
            get_chart_node("Aggregated Ratings", 5, 59),
            get_chart_node("Average Rating by Movie", 5, 59),
            get_chart_node("Total Movies Watched", 2, 59)
        ]
        
        row2_charts = [
            get_chart_node("User-Movie Interaction Matrix", 5, 57),
            get_chart_node("Users by State", 4, 57),
            get_chart_node("Movies by Genre", 3, 57)
        ]
        
        row1_charts = [c for c in row1_charts if c]
        row2_charts = [c for c in row2_charts if c]
        
        ROOT_ID = "ROOT_ID"
        GRID_ID = "GRID_ID"
        ROW1_ID = f"ROW-{uuid.uuid4().hex[:10]}"
        ROW2_ID = f"ROW-{uuid.uuid4().hex[:10]}"
        
        layout = {
            ROOT_ID: {"children": [GRID_ID], "id": ROOT_ID, "type": "ROOT"},
            GRID_ID: {
                "children": [ROW1_ID, ROW2_ID],
                "id": GRID_ID,
                "parents": [ROOT_ID],
                "type": "GRID"
            },
            ROW1_ID: {
                "children": [c["id"] for c in row1_charts],
                "id": ROW1_ID,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": [ROOT_ID, GRID_ID],
                "type": "ROW"
            },
            ROW2_ID: {
                "children": [c["id"] for c in row2_charts],
                "id": ROW2_ID,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": [ROOT_ID, GRID_ID],
                "type": "ROW"
            }
        }
        
        for c in row1_charts:
            c["parents"] = [ROOT_ID, GRID_ID, ROW1_ID]
            layout[c["id"]] = c
            
        for c in row2_charts:
            c["parents"] = [ROOT_ID, GRID_ID, ROW2_ID]
            layout[c["id"]] = c
        
        dashboard.position_json = json.dumps(layout)
        db.session.commit()
        print(f"Dashboard layout updated")
        
    except Exception as e:
        print("EXCEPTION_OCCURRED")
        traceback.print_exc()
        sys.exit(1)
'''

    success, stdout, stderr = run_python_in_container(script)

    if success:
        log_success("Dashboard layout updated!")
    else:
        log_warning(f"Layout update problem: {stdout} {stderr}")


def main():
    parser = argparse.ArgumentParser(description='Setup Superset for Dataflix')
    parser.add_argument('--clean', action='store_true', help='Clean existing configuration before setup')
    args = parser.parse_args()

    print(f"\n{BLUE}{'='*60}{END}")
    print(f"{BLUE}  Dataflix - Superset Configuration{END}")
    print(f"{BLUE}{'='*60}{END}\n")

    # Wait for Superset to be available
    if not wait_for_superset():
        log_error("Could not connect to Superset")
        return 1

    if args.clean:
        clean_superset()

    # Configure components
    setup_database()
    setup_datasets()
    setup_dashboard()
    setup_charts()
    update_dashboard_layout()

    print(f"\n{GREEN}{'='*60}{END}")
    print(f"{GREEN}  Superset Configuration Completed!{END}")
    print(f"{GREEN}{'='*60}{END}\n")
    print(f"Access Superset at: {SUPERSET_URL}")
    print(f"User: admin")
    print(f"Password: admin\n")

    return 0


if __name__ == '__main__':
    sys.exit(main())
