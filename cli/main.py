#!/usr/bin/env python3
"""
Dataflix CLI - Command Line Interface
Manages users, movies, ratings and recommendations
"""

import requests
import json
import os
from typing import Optional, List, Dict
from datetime import datetime
import sys

# Configurations
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:5002')

# ============================================================================
# ANSI Colors
# ============================================================================
class Colors:
    # Basic colors
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'

    # Text colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'

    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'

    # Bright colors
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'


class DataflixCLI:
    """Dataflix CLI Client"""

    def __init__(self):
        self.base_url = API_BASE_URL
        self.current_user = None
        self.page_size = 10

    # ========================================================================
    # Display Utilities
    # ========================================================================

    def clear_screen(self):
        """Clears the screen"""
        os.system('clear' if os.name != 'nt' else 'cls')

    def print_header(self, text: str, color: str = Colors.CYAN):
        """Prints stylized header"""
        width = 60
        print(f"\n{color}{Colors.BOLD}{'═' * width}")
        print(f"  {text.center(width - 4)}")
        print(f"{'═' * width}{Colors.RESET}\n")

    def print_subheader(self, text: str):
        """Prints subheader"""
        print(f"\n{Colors.YELLOW}{Colors.BOLD}── {text} ──{Colors.RESET}\n")

    def print_success(self, text: str):
        """Prints success message"""
        print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")

    def print_error(self, text: str):
        """Prints error message"""
        print(f"{Colors.RED}✗ {text}{Colors.RESET}")

    def print_warning(self, text: str):
        """Prints warning"""
        print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")

    def print_info(self, text: str):
        """Prints information"""
        print(f"{Colors.BLUE}ℹ {text}{Colors.RESET}")

    def print_movie_card(self, movie: dict, index: int = None, show_score: bool = False):
        """Prints stylized movie card"""
        idx_str = f"{Colors.BRIGHT_CYAN}{index:2d}.{Colors.RESET} " if index else "   "

        # Title and year
        title = movie.get('title', 'N/A')
        year = movie.get('release_year', 'N/A')
        genre = movie.get('genre', 'N/A')

        print(f"{idx_str}{Colors.BOLD}{Colors.WHITE}{title}{Colors.RESET} {Colors.DIM}({year}){Colors.RESET}")

        # Genre and director
        director = movie.get('director', '')
        print(f"    {Colors.MAGENTA}🎬 {genre}{Colors.RESET}", end='')
        if director:
            print(f" {Colors.DIM}• Director: {director}{Colors.RESET}", end='')
        print()

        # IMDB and duration
        imdb = movie.get('imdb_rating', 0)
        duration = movie.get('duration_minutes', 0)

        # Stars based on IMDB
        stars = self.get_star_rating(imdb / 2)  # IMDB is out of 10, convert to 5

        print(f"    {Colors.YELLOW}⭐ IMDB: {imdb}/10{Colors.RESET} {stars}", end='')
        if duration:
            print(f" {Colors.DIM}• {duration}min{Colors.RESET}", end='')

        # ML Score if available
        if show_score and 'score' in movie:
            try:
                score = float(movie['score'])
                print(f" {Colors.CYAN}• ML: {score:.2f}/5{Colors.RESET}", end='')
            except:
                pass

        # Average rating if available
        if 'avg_rating' in movie and movie['avg_rating']:
            try:
                avg = float(movie['avg_rating'])
                print(f" {Colors.GREEN}• Avg: {avg:.1f}/5{Colors.RESET}", end='')
            except:
                pass

        print("\n")

    def get_star_rating(self, rating: float) -> str:
        """Returns visual star representation"""
        full_stars = int(rating)
        half_star = 1 if rating - full_stars >= 0.5 else 0
        empty_stars = 5 - full_stars - half_star
        return f"{Colors.YELLOW}{'★' * full_stars}{'½' if half_star else ''}{'☆' * empty_stars}{Colors.RESET}"

    def print_menu(self, options: List[str], title: str = None):
        """Prints stylized menu"""
        if title:
            print(f"{Colors.BOLD}{title}{Colors.RESET}\n")

        for i, option in enumerate(options):
            if option.startswith('-'):  # Separator
                print(f"{Colors.DIM}{'─' * 40}{Colors.RESET}")
            elif option == '':
                print()
            else:
                # Detect if it's an exit/back option
                num = option.split('.')[0].strip() if '.' in option else str(i)
                text = option.split('.', 1)[1].strip() if '.' in option else option

                if num == '0' or 'exit' in text.lower() or 'back' in text.lower() or 'sair' in text.lower() or 'voltar' in text.lower():
                    print(f"  {Colors.RED}{num}.{Colors.RESET} {Colors.DIM}{text}{Colors.RESET}")
                else:
                    print(f"  {Colors.CYAN}{num}.{Colors.RESET} {text}")
        print()

    def get_input(self, prompt: str = "Select", options: List[str] = None) -> str:
        """Gets user input with style"""
        try:
            if options:
                opts_str = f"{Colors.DIM}[{'/'.join(options)}]{Colors.RESET}"
                return input(f"{Colors.BRIGHT_WHITE}{prompt} {opts_str}: {Colors.RESET}").strip()
            return input(f"{Colors.BRIGHT_WHITE}{prompt}: {Colors.RESET}").strip()
        except EOFError:
            return '0'

    def confirm(self, message: str) -> bool:
        """Asks for user confirmation"""
        response = self.get_input(f"{message}", ['y', 'n']).lower()
        return response == 'y' or response == 's'

    def wait_key(self):
        """Waits for key press to continue"""
        input(f"\n{Colors.DIM}Press Enter to continue...{Colors.RESET}")

    def print_pagination_info(self, current_page: int, total_pages: int, total_items: int):
        """Prints pagination info"""
        print(f"{Colors.DIM}Page {current_page}/{total_pages} • Total: {total_items} items{Colors.RESET}")
        print(f"{Colors.DIM}[n] next • [p] previous • [number] select • [0] back{Colors.RESET}\n")

    # ========================================================================
    # API Requests
    # ========================================================================

    def make_request(self, method: str, endpoint: str, data: Optional[dict] = None):
        """Makes API request"""
        try:
            url = f"{self.base_url}{endpoint}"

            if method.upper() == 'GET':
                response = requests.get(url, timeout=10)
            elif method.upper() == 'POST':
                response = requests.post(url, json=data, timeout=10)
            else:
                self.print_error(f"HTTP method not supported: {method}")
                return None

            if response.status_code >= 400:
                self.print_error(f"API Error: {response.status_code}")
                if response.text:
                    print(f"{Colors.DIM}{response.text[:200]}{Colors.RESET}")
                return None

            return response.json()

        except requests.exceptions.ConnectionError:
            self.print_error("Could not connect to API.")
            self.print_info("Check if backend is running.")
            return None
        except requests.exceptions.Timeout:
            self.print_error("Request timeout.")
            return None
        except Exception as e:
            self.print_error(f"Request error: {str(e)}")
            return None

    # ========================================================================
    # User Management
    # ========================================================================

    def select_or_create_user(self):
        """Selects or creates a user"""
        self.clear_screen()
        self.print_header("👤 User Selection", Colors.BLUE)

        result = self.make_request('GET', '/users')

        if not result or not result.get('data'):
            self.print_info("No users found. Let's create a new one!")
            self.wait_key()
            self.create_user()
            return

        users = result['data']

        self.print_subheader("Available Users")

        for i, user in enumerate(users, 1):
            status = f"{Colors.GREEN}●{Colors.RESET}" if self.current_user and self.current_user['user_id'] == user['user_id'] else f"{Colors.DIM}○{Colors.RESET}"
            print(f"  {Colors.CYAN}{i:2d}.{Colors.RESET} {status} {Colors.BOLD}{user['username']}{Colors.RESET}")
            print(f"      {Colors.DIM}{user['first_name']} {user['last_name']} • {user['city']}, {user['state']}{Colors.RESET}\n")

        print(f"  {Colors.GREEN}{len(users) + 1:2d}.{Colors.RESET} ➕ Create new user")
        print(f"  {Colors.RED} 0.{Colors.RESET} {Colors.DIM}Back{Colors.RESET}\n")

        choice = self.get_input("Select a user")

        try:
            choice = int(choice)

            if choice == 0:
                return
            elif choice == len(users) + 1:
                self.create_user()
            elif 1 <= choice <= len(users):
                self.current_user = users[choice - 1]
                self.print_success(f"User selected: {self.current_user['username']}")
                self.wait_key()
            else:
                self.print_error("Invalid option")
                self.wait_key()

        except ValueError:
            self.print_error("Invalid option")
            self.wait_key()

    def create_user(self):
        """Creates a new user"""
        self.clear_screen()
        self.print_header("➕ Create New User", Colors.GREEN)

        print(f"{Colors.DIM}Fill in the details below (or leave empty to cancel){Colors.RESET}\n")

        username = self.get_input("Username")
        if not username:
            self.print_warning("Creation cancelled")
            return

        email = self.get_input("Email")
        first_name = self.get_input("First name")
        last_name = self.get_input("Last name")
        city = self.get_input("City")
        state = self.get_input("State (abbr)")
        country = self.get_input("Country") or "Brazil"
        age = self.get_input("Age (optional)")

        data = {
            'username': username,
            'email': email,
            'first_name': first_name,
            'last_name': last_name,
            'city': city,
            'state': state,
            'country': country,
            'age': int(age) if age.isdigit() else 0
        }

        result = self.make_request('POST', '/users', data)

        if result and result.get('status') == 'success':
            self.current_user = result['data']
            self.print_success(f"User created: {self.current_user['username']}")
        else:
            self.print_error("Error creating user")

        self.wait_key()

    def print_user_status(self):
        """Prints current user status"""
        if self.current_user:
            print(f"{Colors.BG_BLUE}{Colors.WHITE} 👤 {self.current_user['username']} {Colors.RESET} ", end='')
            print(f"{Colors.DIM}{self.current_user['first_name']} {self.current_user['last_name']} • {self.current_user['city']}, {self.current_user['state']}{Colors.RESET}")
        else:
            print(f"{Colors.BG_YELLOW}{Colors.BLACK} ⚠ No user selected {Colors.RESET}")

    # ========================================================================
    # Movie Navigation
    # ========================================================================

    def browse_movies(self):
        """Browses movies with pagination"""
        if not self.current_user:
            self.print_error("Select a user first")
            self.wait_key()
            return

        page = 1
        genre_filter = None

        while True:
            self.clear_screen()
            self.print_header("🎬 Movie Catalog", Colors.MAGENTA)
            self.print_user_status()

            if genre_filter:
                print(f"{Colors.DIM}Filter: {genre_filter}{Colors.RESET}")

            # Fetch movies
            params = f"?limit={self.page_size}&offset={(page - 1) * self.page_size}"
            if genre_filter:
                params += f"&genre={genre_filter}"

            result = self.make_request('GET', f'/movies{params}')

            if not result or not result.get('data'):
                self.print_info("No movies found")
                self.wait_key()
                return

            movies = result['data']
            total = result.get('count', len(movies))
            total_pages = max(1, (total + self.page_size - 1) // self.page_size)

            self.print_subheader(f"Available Movies")

            for i, movie in enumerate(movies, 1):
                self.print_movie_card(movie, (page - 1) * self.page_size + i)

            # Pagination
            print(f"\n{Colors.DIM}{'─' * 60}{Colors.RESET}")
            print(f"{Colors.DIM}Page {page}/{total_pages} • Showing {len(movies)} movies{Colors.RESET}")
            print()

            options = []
            if page < total_pages:
                options.append("[n] Next")
            if page > 1:
                options.append("[p] Previous")
            options.append("[f] Filter genre")
            options.append("[number] View movie")
            options.append("[0] Back")

            print(f"{Colors.DIM}{' • '.join(options)}{Colors.RESET}\n")

            choice = self.get_input("Option").lower()

            if choice == '0':
                return
            elif choice == 'n' and page < total_pages:
                page += 1
            elif choice == 'p' and page > 1:
                page -= 1
            elif choice == 'f':
                new_filter = self.get_input("Genre (empty to clear)")
                genre_filter = new_filter if new_filter else None
                page = 1
            else:
                try:
                    idx = int(choice)
                    movie_idx = idx - (page - 1) * self.page_size - 1
                    if 0 <= movie_idx < len(movies):
                        self.manage_movie(movies[movie_idx])
                except ValueError:
                    pass

    def manage_movie(self, movie: dict):
        """Manages a specific movie"""
        while True:
            self.clear_screen()
            self.print_header(f"🎬 {movie['title']}", Colors.MAGENTA)

            # Full movie details
            print(f"{Colors.BOLD}Movie Information{Colors.RESET}\n")

            print(f"  {Colors.CYAN}Title:{Colors.RESET}    {movie['title']}")
            print(f"  {Colors.CYAN}Genre:{Colors.RESET}    {movie.get('genre', 'N/A')}")
            print(f"  {Colors.CYAN}Year:{Colors.RESET}       {movie.get('release_year', 'N/A')}")
            print(f"  {Colors.CYAN}Director:{Colors.RESET}   {movie.get('director', 'N/A')}")
            print(f"  {Colors.CYAN}Duration:{Colors.RESET}   {movie.get('duration_minutes', 'N/A')} minutes")
            print(f"  {Colors.CYAN}IMDB:{Colors.RESET}      {movie.get('imdb_rating', 'N/A')}/10 {self.get_star_rating(movie.get('imdb_rating', 0) / 2)}")

            if movie.get('description'):
                print(f"\n  {Colors.CYAN}Synopsis:{Colors.RESET}")
                # Break description into lines
                desc = movie['description']
                words = desc.split()
                line = "  "
                for word in words:
                    if len(line) + len(word) > 58:
                        print(f"{Colors.DIM}{line}{Colors.RESET}")
                        line = "  "
                    line += word + " "
                if line.strip():
                    print(f"{Colors.DIM}{line}{Colors.RESET}")

            print()

            self.print_menu([
                "1. 👁️  Mark as watched",
                "2. ⭐ Rate movie",
                "-",
                "0. Back"
            ])

            choice = self.get_input("Option")

            if choice == '0':
                return
            elif choice == '1':
                self.mark_watched(movie)
            elif choice == '2':
                self.rate_movie(movie)

    def mark_watched(self, movie: dict):
        """Marks movie as watched"""
        data = {
            'user_id': self.current_user['user_id'],
            'movie_id': movie['movie_id']
        }

        result = self.make_request('POST', '/watched', data)

        if result and result.get('status') == 'success':
            self.print_success(f"Movie '{movie['title']}' marked as watched!")
        else:
            self.print_error("Error marking movie as watched")

        self.wait_key()

    def rate_movie(self, movie: dict):
        """Rates a movie"""
        self.print_subheader("Rate Movie")

        print(f"{Colors.DIM}Scale: 1 (disliked) to 5 (loved){Colors.RESET}")
        print()
        print(f"  {Colors.RED}1{Colors.RESET} ★☆☆☆☆  Disliked")
        print(f"  {Colors.YELLOW}2{Colors.RESET} ★★☆☆☆  Regular")
        print(f"  {Colors.YELLOW}3{Colors.RESET} ★★★☆☆  Good")
        print(f"  {Colors.GREEN}4{Colors.RESET} ★★★★☆  Very good")
        print(f"  {Colors.GREEN}5{Colors.RESET} ★★★★★  Excellent")
        print()

        rating_str = self.get_input("Your rating (1-5)")

        try:
            rating = float(rating_str)

            if not (1 <= rating <= 5):
                self.print_error("Rating must be between 1 and 5")
                self.wait_key()
                return

            liked = rating >= 4

            data = {
                'user_id': self.current_user['user_id'],
                'movie_id': movie['movie_id'],
                'rating': rating,
                'liked': liked
            }

            result = self.make_request('POST', '/ratings', data)

            if result and result.get('status') == 'success':
                emoji = "👍" if liked else "👎"
                self.print_success(f"Rating registered: {rating}/5 {emoji}")
            else:
                self.print_error("Error registering rating")

        except ValueError:
            self.print_error("Invalid rating")

        self.wait_key()

    # ========================================================================
    # Recommendations
    # ========================================================================

    def view_recommendations(self):
        """Views recommendations with interaction option"""
        if not self.current_user:
            self.print_error("Select a user first")
            self.wait_key()
            return

        while True:
            self.clear_screen()
            self.print_header("🎯 Personalized Recommendations", Colors.GREEN)
            self.print_user_status()
            print()

            result = self.make_request('GET', f"/recommendations/{self.current_user['user_id']}?limit=10")

            if not result or not result.get('data'):
                self.print_info("No recommendations available.")
                self.print_info("Watch and rate more movies to receive recommendations!")
                self.wait_key()
                return

            recommendations = result['data']
            source = result.get('source', 'unknown')
            model_run_id = result.get('model_run_id')

            # Show recommendation source
            if source == 'ml_model_nmf':
                print(f"{Colors.GREEN}📊 Source: ML Model (Collaborative Filtering){Colors.RESET}")
                if model_run_id:
                    print(f"{Colors.DIM}   Run ID: {model_run_id[:8]}...{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}📋 Source: Top Rated (SQL Fallback){Colors.RESET}")

            self.print_subheader(f"Top {len(recommendations)} for you")

            for i, movie in enumerate(recommendations, 1):
                self.print_movie_card(movie, i, show_score=True)

            print(f"{Colors.DIM}{'─' * 60}{Colors.RESET}")
            print(f"{Colors.DIM}[number] View/Watch movie • [0] Back{Colors.RESET}\n")

            choice = self.get_input("Option")

            if choice == '0':
                return

            try:
                idx = int(choice)
                if 1 <= idx <= len(recommendations):
                    self.manage_movie(recommendations[idx - 1])
            except ValueError:
                pass

    # ========================================================================
    # User History
    # ========================================================================

    def view_watched_movies(self):
        """Visualizes watched movies"""
        if not self.current_user:
            self.print_error("Select a user first")
            self.wait_key()
            return

        self.clear_screen()
        self.print_header("👁️ Watched Movies", Colors.BLUE)
        self.print_user_status()
        print()

        result = self.make_request('GET', f"/watched/{self.current_user['user_id']}")

        if not result or not result.get('data'):
            self.print_info("No movies watched yet")
            self.print_info("Explore the catalog and start watching!")
            self.wait_key()
            return

        movies = result['data']

        print(f"{Colors.DIM}Total: {len(movies)} watched movies{Colors.RESET}\n")

        for i, movie in enumerate(movies, 1):
            self.print_movie_card(movie, i)

        self.wait_key()

    def view_ratings(self):
        """Visualizes user ratings"""
        if not self.current_user:
            self.print_error("Select a user first")
            self.wait_key()
            return

        self.clear_screen()
        self.print_header("⭐ My Ratings", Colors.YELLOW)
        self.print_user_status()
        print()

        result = self.make_request('GET', f"/ratings/{self.current_user['user_id']}")

        if not result or not result.get('data'):
            self.print_info("No ratings registered yet")
            self.print_info("Watch movies and leave your opinion!")
            self.wait_key()
            return

        ratings = result['data']

        print(f"{Colors.DIM}Total: {len(ratings)} ratings{Colors.RESET}\n")

        for i, rating in enumerate(ratings, 1):
            liked_emoji = "👍" if rating['liked'] else "👎"
            stars = self.get_star_rating(rating['rating'])

            print(f"  {Colors.CYAN}{i:2d}.{Colors.RESET} {Colors.BOLD}{rating['title']}{Colors.RESET}")
            print(f"      {stars} {Colors.BOLD}{rating['rating']}/5{Colors.RESET} {liked_emoji}")
            print(f"      {Colors.DIM}{rating.get('genre', '')} • {rating.get('rated_at', '')[:10] if rating.get('rated_at') else ''}{Colors.RESET}\n")

        self.wait_key()

    # ========================================================================
    # Pipeline
    # ========================================================================

    def trigger_pipeline(self):
        """Executes ELT pipeline (Airflow DAG)"""
        self.clear_screen()
        self.print_header("🔄 Execute ELT Pipeline", Colors.CYAN)

        print(f"{Colors.BOLD}This pipeline will:{Colors.RESET}\n")
        print(f"  {Colors.CYAN}1.{Colors.RESET} Extract data from PostgreSQL")
        print(f"  {Colors.CYAN}2.{Colors.RESET} Load into ClickHouse (with masking)")
        print(f"  {Colors.CYAN}3.{Colors.RESET} Execute dbt transformations")
        print(f"  {Colors.CYAN}4.{Colors.RESET} Train recommendation model in MLflow")
        print()

        if not self.confirm("Do you want to execute the pipeline?"):
            self.print_info("Operation cancelled")
            self.wait_key()
            return

        print()
        self.print_info("Triggering pipeline...")

        try:
            airflow_url = os.getenv('AIRFLOW_URL', 'http://localhost:8080')
            airflow_user = os.getenv('AIRFLOW_USER', 'admin')
            airflow_password = os.getenv('AIRFLOW_PASSWORD', 'admin')

            response = requests.post(
                f"{airflow_url}/api/v1/dags/elt_pipeline/dagRuns",
                auth=(airflow_user, airflow_password),
                json={"conf": {}},
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code in [200, 201]:
                result = response.json()
                run_id = result.get('dag_run_id', 'unknown')
                self.print_success("Pipeline triggered successfully!")
                self.print_info(f"DAG Run ID: {run_id}")
                self.print_info(f"Track at: {airflow_url}/dags/elt_pipeline/grid")
            elif response.status_code == 401:
                self.print_error("Authentication error in Airflow")
                self.print_info("Check AIRFLOW_USER and AIRFLOW_PASSWORD credentials")
            elif response.status_code == 404:
                self.print_error("DAG not found")
            else:
                self.print_error(f"Error triggering pipeline: {response.status_code}")
                if response.text:
                    print(f"{Colors.DIM}{response.text[:200]}{Colors.RESET}")

        except requests.exceptions.ConnectionError:
            self.print_error("Could not connect to Airflow")
            self.print_info("Check if Airflow is running")
        except Exception as e:
            self.print_error(f"Error: {str(e)}")

        self.wait_key()

    # ========================================================================
    # Main Menu
    # ========================================================================

    def main_menu(self):
        """Main menu"""
        while True:
            self.clear_screen()

            # Logo
            print(f"""
{Colors.CYAN}{Colors.BOLD}
    ██████╗  █████╗ ████████╗ █████╗ ███████╗██╗     ██╗██╗  ██╗
    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝██║     ██║╚██╗██╔╝
    ██║  ██║███████║   ██║   ███████║█████╗  ██║     ██║ ╚███╔╝
    ██║  ██║██╔══██║   ██║   ██╔══██║██╔══╝  ██║     ██║ ██╔██╗
    ██████╔╝██║  ██║   ██║   ██║  ██║██║     ███████╗██║██╔╝ ██╗
    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝╚═╝  ╚═╝
{Colors.RESET}""")
            print(f"{Colors.DIM}    Movie Recommendation System{Colors.RESET}\n")

            # User status
            self.print_user_status()
            print()

            # Menu
            self.print_menu([
                "1. 👤 Select/Create User",
                "2. 🎬 Browse Movie Catalog",
                "3. 🎯 View Recommendations",
                "4. 👁️  View Watched Movies",
                "5. ⭐ View My Ratings",
                "-",
                "6. 🔄 Execute Pipeline (DAG)",
                "-",
                "0. 🚪 Exit"
            ])

            choice = self.get_input("Option")

            if choice == '1':
                self.select_or_create_user()
            elif choice == '2':
                self.browse_movies()
            elif choice == '3':
                self.view_recommendations()
            elif choice == '4':
                self.view_watched_movies()
            elif choice == '5':
                self.view_ratings()
            elif choice == '6':
                self.trigger_pipeline()
            elif choice == '0':
                self.clear_screen()
                print(f"\n{Colors.CYAN}See you soon! 👋{Colors.RESET}\n")
                break
            elif choice.lower() == 'q':
                self.clear_screen()
                print(f"\n{Colors.CYAN}See you soon! 👋{Colors.RESET}\n")
                break


def main():
    """Main function"""
    try:
        cli = DataflixCLI()
        cli.main_menu()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Program interrupted by user{Colors.RESET}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Colors.RED}Error: {str(e)}{Colors.RESET}")
        sys.exit(1)


if __name__ == '__main__':
    main()
