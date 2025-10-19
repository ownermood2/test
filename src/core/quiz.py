"""Quiz Manager for Telegram Quiz Bot.

This module provides the core quiz management functionality including question
selection, user scoring, leaderboards, and statistics tracking. Uses PostgreSQL
database for persistent storage with intelligent in-memory caching for optimal
performance.
"""

import json
import random
import logging
import traceback
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque
from src.core.database import DatabaseManager
from src.core.exceptions import QuestionNotFoundError, ValidationError, DatabaseError

logger = logging.getLogger(__name__)

class QuizManager:
    """Manages quiz operations, scoring, and statistics.
    
    This class serves as the central coordinator for all quiz-related operations.
    It handles question management, user score tracking, leaderboard generation,
    and comprehensive statistics. Uses PostgreSQL database for persistent storage
    with intelligent in-memory caching for optimal performance.
    
    Key features:
    - Intelligent question selection avoiding recently asked questions
    - Real-time score tracking and leaderboard updates
    - Group and private chat statistics
    - Question caching for improved performance
    - Pure PostgreSQL storage with no file I/O
    
    Attributes:
        db (DatabaseManager): Database manager instance
        questions (List[Dict]): Cached quiz questions from database
        scores (Dict): User scores dictionary (in-memory)
        active_chats (List): List of active chat IDs (in-memory)
        stats (Dict): User statistics dictionary (in-memory)
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the quiz manager with database connection and caching structures.
        
        Loads questions from PostgreSQL database and sets up in-memory caching
        for optimal performance. No file I/O operations.
        
        Args:
            db_manager (DatabaseManager, optional): Shared database manager instance.
                                                   Creates new one if not provided.
        
        Raises:
            DatabaseError: If database initialization or data loading fails
        """
        # Initialize in-memory data structures
        self.questions = []
        self.scores = {}
        self.active_chats = []
        self.stats = {}

        # Use provided database manager or create new one
        self.db = db_manager if db_manager else DatabaseManager()
        logger.info("Database connection initialized in QuizManager")

        # Initialize caching structures
        self._cached_questions = None
        self._cached_leaderboard = None
        self._leaderboard_cache_time = None
        self._cache_duration = timedelta(minutes=5)

        # Initialize tracking structures
        self.recent_questions = defaultdict(lambda: deque(maxlen=50))
        self.last_question_time = defaultdict(dict)
        self.available_questions = defaultdict(list)

        # Load questions from database
        try:
            db_questions = self.db.get_all_questions()
            self.questions = []
            for db_q in db_questions:
                self.questions.append({
                    'id': db_q['id'],
                    'question': db_q['question'],
                    'options': db_q['options'],
                    'correct_answer': db_q['correct_answer']
                })
            logger.info(f"Successfully loaded {len(self.questions)} questions from database")
        except Exception as e:
            logger.error(f"Failed to load questions from database: {e}")
            raise DatabaseError(f"Failed to initialize questions from database: {e}") from e


    def _init_user_stats(self, user_id: str) -> None:
        """Initialize stats for a new user with enhanced tracking.
        
        Creates a complete stats structure for new users including daily
        activity, category scores, streaks, and group participation.
        
        Args:
            user_id (str): User ID as string
        """
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.stats[user_id] = {
            'total_quizzes': 0,
            'correct_answers': 0,
            'current_streak': 0,
            'longest_streak': 0,
            'last_correct_date': None,
            'category_scores': {},
            'daily_activity': {
                current_date: {
                    'attempts': 0,
                    'correct': 0
                }
            },
            'last_quiz_date': current_date,
            'last_activity_date': current_date,
            'join_date': current_date,
            'groups': {},
            'private_chat_activity': {
                'total_messages': 0,
                'last_active': current_date
            }
        }

    def get_user_stats(self, user_id: int) -> Dict:
        """Get comprehensive stats for a user.
        
        Retrieves and calculates user statistics including total quizzes,
        correct answers, success rate, streaks, and activity metrics.
        Always returns a valid dictionary, even for new users.
        
        Args:
            user_id (int): Telegram user ID
        
        Returns:
            Dict: User statistics dictionary with keys:
                - total_quizzes: Total quiz attempts
                - correct_answers: Number of correct answers
                - success_rate: Percentage of correct answers
                - current_score: Current score
                - today_quizzes: Attempts today
                - week_quizzes: Attempts this week
                - month_quizzes: Attempts this month
                - current_streak: Current correct answer streak
                - longest_streak: Best streak achieved
        """
        try:
            user_id_str = str(user_id)
            current_date = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"Attempting to get stats for user {user_id}")
            logger.debug(f"Current stats data: {self.stats.get(user_id_str, 'Not Found')}")

            # Initialize stats if user doesn't exist
            if user_id_str not in self.stats:
                logger.info(f"Initializing new stats for user {user_id}")
                self._init_user_stats(user_id_str)

                # Return initial stats
                return {
                    'total_quizzes': 0,
                    'correct_answers': 0,
                    'success_rate': 0.0,
                    'today_quizzes': 0,
                    'week_quizzes': 0,
                    'month_quizzes': 0,
                    'current_score': 0,
                    'current_streak': 0,
                    'longest_streak': 0
                }

            stats = self.stats[user_id_str]
            logger.debug(f"Retrieved raw stats: {stats}")

            # Ensure today's activity exists
            if current_date not in stats['daily_activity']:
                stats['daily_activity'][current_date] = {'attempts': 0, 'correct': 0}

            # Get today's stats
            today_stats = stats['daily_activity'].get(current_date, {'attempts': 0, 'correct': 0})

            # Calculate weekly stats
            week_start = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime('%Y-%m-%d')
            week_quizzes = sum(
                day_stats['attempts']
                for date, day_stats in stats['daily_activity'].items()
                if date >= week_start
            )

            # Calculate monthly stats
            month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
            month_quizzes = sum(
                day_stats['attempts']
                for date, day_stats in stats['daily_activity'].items()
                if date >= month_start
            )

            # Calculate success rate
            if stats['total_quizzes'] > 0:
                success_rate = (stats['correct_answers'] / stats['total_quizzes']) * 100
            else:
                success_rate = 0.0

            # Sync with scores data
            score = self.scores.get(user_id_str, 0)
            if score != stats['correct_answers']:
                logger.info(f"Syncing score for user {user_id}: {score} != {stats['correct_answers']}")
                stats['correct_answers'] = score
                stats['total_quizzes'] = max(stats['total_quizzes'], score)

            formatted_stats = {
                'total_quizzes': stats['total_quizzes'],
                'correct_answers': stats['correct_answers'],
                'success_rate': round(success_rate, 1),
                'current_score': stats['correct_answers'],
                'today_quizzes': today_stats['attempts'],
                'week_quizzes': week_quizzes,
                'month_quizzes': month_quizzes,
                'current_streak': stats.get('current_streak', 0),
                'longest_streak': stats.get('longest_streak', 0)
            }

            logger.info(f"Successfully retrieved stats for user {user_id}: {formatted_stats}")
            return formatted_stats

        except Exception as e:
            logger.error(f"Error getting stats for user {user_id}: {str(e)}\n{traceback.format_exc()}")
            logger.error(f"Raw stats data: {self.stats.get(str(user_id), 'Not Found')}")
            # Always return a valid dict, never None
            return {
                'total_quizzes': 0,
                'correct_answers': 0,
                'success_rate': 0.0,
                'today_quizzes': 0,
                'week_quizzes': 0,
                'month_quizzes': 0,
                'current_score': 0,
                'current_streak': 0,
                'longest_streak': 0
            }

    def get_group_leaderboard(self, chat_id: int) -> Dict:
        """Get group-specific leaderboard with detailed analytics.
        
        Generates comprehensive leaderboard and statistics for a specific group
        chat, including user rankings, activity metrics, and group performance.
        
        Args:
            chat_id (int): Telegram chat ID
        
        Returns:
            Dict: Leaderboard data with keys:
                - total_quizzes: Total group quiz attempts
                - total_correct: Total correct answers
                - group_accuracy: Overall success rate
                - active_users: Activity breakdown (today/week/month/total)
                - leaderboard: Top 20 users with stats
                - group_streak: Group's active streak
        """
        chat_id_str = str(chat_id)
        current_date = datetime.now()
        today = current_date.strftime('%Y-%m-%d')
        week_start = (current_date - timedelta(days=current_date.weekday())).strftime('%Y-%m-%d')
        month_start = current_date.replace(day=1).strftime('%Y-%m-%d')

        # Initialize counters and sets
        total_group_quizzes = 0
        total_correct_answers = 0
        active_users = {
            'today': set(),
            'week': set(),
            'month': set(),
            'total': set()
        }
        leaderboard = []

        # Process user stats
        for user_id, stats in self.stats.items():
            if chat_id_str in stats.get('groups', {}):
                group_stats = stats['groups'][chat_id_str]
                active_users['total'].add(user_id)

                # Update activity counters
                last_activity = group_stats.get('last_activity_date')
                if last_activity:
                    if last_activity == today:
                        active_users['today'].add(user_id)
                    if last_activity >= week_start:
                        active_users['week'].add(user_id)
                    if last_activity >= month_start:
                        active_users['month'].add(user_id)

                # Calculate user statistics
                user_total_attempts = group_stats.get('total_quizzes', 0)
                user_correct_answers = group_stats.get('correct_answers', 0)
                total_group_quizzes += user_total_attempts
                total_correct_answers += user_correct_answers

                # Get daily activity stats
                daily_stats = group_stats.get('daily_activity', {})
                today_stats = daily_stats.get(today, {'attempts': 0, 'correct': 0})

                leaderboard.append({
                    'user_id': int(user_id),
                    'total_attempts': user_total_attempts,
                    'correct_answers': user_correct_answers,
                    'wrong_answers': user_total_attempts - user_correct_answers,
                    'accuracy': round((user_correct_answers / user_total_attempts * 100) if user_total_attempts > 0 else 0, 1),
                    'score': group_stats.get('score', 0),
                    'current_streak': group_stats.get('current_streak', 0),
                    'longest_streak': group_stats.get('longest_streak', 0),
                    'today_attempts': today_stats['attempts'],
                    'today_correct': today_stats['correct'],
                    'last_active': group_stats.get('last_activity_date', 'Never')
                })

        # Sort leaderboard by correct_answers DESC, then total_attempts DESC (as per requirements)
        leaderboard.sort(key=lambda x: (x['correct_answers'], x['total_attempts']), reverse=True)
        group_accuracy = (total_correct_answers / total_group_quizzes * 100) if total_group_quizzes > 0 else 0

        return {
            'total_quizzes': total_group_quizzes,
            'total_correct': total_correct_answers,
            'group_accuracy': round(group_accuracy, 1),
            'active_users': {
                'today': len(active_users['today']),
                'week': len(active_users['week']),
                'month': len(active_users['month']),
                'total': len(active_users['total'])
            },
            'leaderboard': leaderboard[:20],  # Top 20 performers for pagination
            'group_streak': 0  # Placeholder for active streak
        }

    def record_group_attempt(self, user_id: int, chat_id: int, is_correct: bool) -> None:
        """Record a quiz attempt for a user in a specific group.
        
        Updates group-specific statistics including attempts, correct answers,
        score, daily activity, and streak tracking.
        
        Args:
            user_id (int): Telegram user ID
            chat_id (int): Telegram chat ID
            is_correct (bool): Whether the answer was correct
        
        Raises:
            DatabaseError: If recording fails
        """
        try:
            user_id_str = str(user_id)
            chat_id_str = str(chat_id)
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Initialize user stats if needed
            if user_id_str not in self.stats:
                self._init_user_stats(user_id_str)

            stats = self.stats[user_id_str]

            # Initialize group stats if needed
            if 'groups' not in stats:
                stats['groups'] = {}

            if chat_id_str not in stats['groups']:
                stats['groups'][chat_id_str] = {
                    'total_quizzes': 0,
                    'correct_answers': 0,
                    'score': 0,
                    'last_activity_date': None,
                    'daily_activity': {},
                    'current_streak': 0,
                    'longest_streak': 0,
                    'last_correct_date': None
                }

            group_stats = stats['groups'][chat_id_str]
            group_stats['total_quizzes'] += 1
            group_stats['last_activity_date'] = current_date

            # Update daily activity
            if current_date not in group_stats['daily_activity']:
                group_stats['daily_activity'][current_date] = {'attempts': 0, 'correct': 0}

            group_stats['daily_activity'][current_date]['attempts'] += 1

            if is_correct:
                group_stats['correct_answers'] += 1
                group_stats['score'] += 1
                group_stats['daily_activity'][current_date]['correct'] += 1

                # Update streak
                if group_stats.get('last_correct_date') == (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'):
                    group_stats['current_streak'] += 1
                else:
                    group_stats['current_streak'] = 1

                group_stats['longest_streak'] = max(group_stats['current_streak'], group_stats['longest_streak'])
                group_stats['last_correct_date'] = current_date
            else:
                group_stats['current_streak'] = 0

            # Group stats recorded in memory (user stats already recorded via increment_score -> record_attempt)
            logger.debug(f"Recorded group attempt for user {user_id} in chat {chat_id} (correct={is_correct})")

        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Failed to record group attempt for user {user_id} in chat {chat_id}: {e}")
            raise DatabaseError(f"Failed to record group attempt: {e}") from e

    def _initialize_available_questions(self, chat_id: int):
        """Initialize or reset available questions pool for a chat.
        
        Creates a shuffled list of all available question indices for the chat.
        
        Args:
            chat_id (int): Telegram chat ID
        """
        self.available_questions[chat_id] = list(range(len(self.questions)))
        random.shuffle(self.available_questions[chat_id])
        logger.info(f"Initialized question pool for chat {chat_id} with {len(self.questions)} questions")

    def get_random_question(self, chat_id: int = 0, category: str = "") -> Optional[Dict[str, Any]]:
        """Get a random question avoiding recent ones with improved tracking and optional category filtering
        
        Args:
            chat_id: Chat ID (use 0 for no specific chat context, negative values are valid for Telegram groups)
            category: Question category filter (use empty string for no filter)
            
        Returns:
            Question dict or None if no questions available
            
        Raises:
            ValueError: If category is invalid type
        """
        # Input validation
        if category is not None and not isinstance(category, str):
            raise ValidationError(f"category must be a string, got {type(category).__name__}")
        
        try:
            if not self.questions:
                logger.warning("No questions available in the quiz database")
                return None

            # Filter questions by category if specified
            if category:
                # Validate category is non-empty
                if not category.strip():
                    raise ValidationError("category must be a non-empty string when provided")
                    
                # Use cached questions filtered by category (PERFORMANCE OPTIMIZATION)
                filtered_questions = [q for q in self.questions if q.get('category') == category]
                if not filtered_questions:
                    logger.warning(f"No questions found for category '{category}'")
                    return None
                
                # If no chat_id (0 means no specific chat), return random from filtered
                if chat_id == 0:
                    selected = random.choice(filtered_questions)
                    return selected
                
                # For chat-specific, use filtered questions
                available_filtered = [q for q in filtered_questions if q['question'] not in self.recent_questions.get(chat_id, [])]
                
                if not available_filtered:
                    # If all category questions were recently used, reset and use any from category
                    available_filtered = filtered_questions
                
                selected = random.choice(available_filtered)
                
                # Track this question
                self.recent_questions[chat_id].append(selected['question'])
                self.last_question_time[chat_id][selected['question']] = datetime.now()
                
                return selected

            # If no chat_id provided (0 means no specific chat), return completely random from cached questions
            if chat_id == 0:
                return random.choice(self.questions) if self.questions else None

            # Use cached questions for chat-specific selection (PERFORMANCE OPTIMIZATION)
            if chat_id not in self.available_questions or not self.available_questions[chat_id]:
                self._initialize_available_questions(chat_id)
            
            question_index = self.available_questions[chat_id].pop()
            question = self.questions[question_index]
            
            if not self.available_questions[chat_id]:
                self._initialize_available_questions(chat_id)
            
            return question

        except Exception as e:
            logger.error(f"Error in get_random_question: {e}\n{traceback.format_exc()}")
            # Fallback to completely random selection if questions available
            if self.questions:
                return random.choice(self.questions)
            return None

    def get_leaderboard(self) -> List[Dict]:
        """Get global leaderboard with caching.
        
        Retrieves top 10 users sorted by score, accuracy, and streak.
        Uses 5-minute cache to reduce computation.
        
        Returns:
            List[Dict]: Top 10 users with their statistics.
        """
        current_time = datetime.now()

        # Force refresh cache if it's stale
        if (self._cached_leaderboard is None or
            self._leaderboard_cache_time is None or
            current_time - self._leaderboard_cache_time > self._cache_duration):

            leaderboard = []
            current_date = current_time.strftime('%Y-%m-%d')

            for user_id, stats in self.stats.items():
                total_attempts = stats['total_quizzes']
                correct_answers = stats['correct_answers']

                # Get today's performance
                today_stats = stats['daily_activity'].get(current_date, {'attempts': 0, 'correct': 0})

                accuracy = (correct_answers / total_attempts * 100) if total_attempts > 0 else 0

                leaderboard.append({
                    'user_id': int(user_id),
                    'total_attempts': total_attempts,
                    'correct_answers': correct_answers,
                    'wrong_answers': total_attempts - correct_answers,
                    'accuracy': round(accuracy, 1),
                    'score': self.get_score(int(user_id)),
                    'today_attempts': today_stats['attempts'],
                    'today_correct': today_stats['correct'],
                    'current_streak': stats.get('current_streak', 0),
                    'longest_streak': stats.get('longest_streak', 0)
                })

            # Sort by score, then accuracy, then streak
            leaderboard.sort(key=lambda x: (-x['score'], -x['accuracy'], -x['current_streak']))
            self._cached_leaderboard = leaderboard[:10]
            self._leaderboard_cache_time = current_time
            logger.info(f"Refreshed leaderboard cache with {len(leaderboard)} entries")

        return self._cached_leaderboard

    def record_attempt(self, user_id: int, is_correct: bool, category: str = ""):
        """Record a quiz attempt for a user in real-time.
        
        Updates user statistics, daily activity, scores, and streaks based on
        the quiz answer.  Does not save to disk to reduce I/O overhead.
        
        Args:
            user_id (int): User's Telegram ID (must be positive).
            is_correct (bool): Whether the answer was correct.
            category (str): Question category. Defaults to empty string.
            
        Raises:
            ValidationError: If user_id is invalid or category is invalid type.
            DatabaseError: If recording fails.
        """
        # Input validation
        if user_id <= 0:
            raise ValidationError(f"user_id must be a positive integer, got {user_id}")
        if category is not None and not isinstance(category, str):
            raise ValidationError(f"category must be a string, got {type(category).__name__}")
            
        try:
            user_id_str = str(user_id)
            current_date = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"Recording attempt for user {user_id}: correct={is_correct}")

            # Initialize user stats if needed
            if user_id_str not in self.stats:
                self._init_user_stats(user_id_str)

            stats = self.stats[user_id_str]
            stats['total_quizzes'] += 1
            stats['last_quiz_date'] = current_date

            # Initialize today's activity if not exists
            if current_date not in stats['daily_activity']:
                stats['daily_activity'][current_date] = {'attempts': 0, 'correct': 0}

            # Update daily activity
            stats['daily_activity'][current_date]['attempts'] += 1

            if is_correct:
                stats['correct_answers'] += 1
                stats['daily_activity'][current_date]['correct'] += 1

                # Update streak
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                if stats.get('last_correct_date') == yesterday:
                    stats['current_streak'] += 1
                else:
                    stats['current_streak'] = 1

                stats['longest_streak'] = max(stats['current_streak'], stats.get('longest_streak', 0))
                stats['last_correct_date'] = current_date

                # Update score
                if user_id_str not in self.scores:
                    self.scores[user_id_str] = 0
                self.scores[user_id_str] += 1

                # Update category scores if provided
                if category:
                    if 'category_scores' not in stats:
                        stats['category_scores'] = {}
                    if category not in stats['category_scores']:
                        stats['category_scores'][category] = 0
                    stats['category_scores'][category] += 1
            else:
                stats['current_streak'] = 0

            logger.info(f"Successfully recorded attempt for user {user_id}: score={self.scores.get(user_id_str)}, streak={stats['current_streak']}")

        except ValidationError:
            raise
        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Failed to record attempt for user {user_id}: {str(e)}\n{traceback.format_exc()}")
            raise DatabaseError(f"Failed to record quiz attempt: {e}") from e

    def add_questions(self, questions_data: List[Dict], allow_duplicates: bool = False) -> Dict:
        """Add multiple questions with validation and duplicate detection
        
        Args:
            questions_data: List of question dictionaries
            allow_duplicates: If True, allows duplicate questions to be added
        
        Returns:
            Dictionary with statistics about added/rejected questions
        """
        stats = {
            'added': 0,
            'rejected': {
                'duplicates': 0,
                'invalid_format': 0,
                'invalid_options': 0
            },
            'errors': [],
            'db_saved': 0,
            'db_failed': 0
        }

        if len(questions_data) > 500:
            stats['errors'].append("Maximum 500 questions allowed at once")
            return stats

        logger.info(f"Starting to add {len(questions_data)} questions. Current count: {len(self.questions)}. Allow duplicates: {allow_duplicates}")
        added_questions = []

        for question_data in questions_data:
            try:
                # Basic format validation
                if not all(key in question_data for key in ['question', 'options', 'correct_answer']):
                    logger.warning(f"Invalid format for question: {question_data}")
                    stats['rejected']['invalid_format'] += 1
                    stats['errors'].append(f"Invalid format for question: {question_data.get('question', 'Unknown')}")
                    continue

                # Clean up question text - remove /addquiz prefix and extra whitespace
                question = question_data['question'].strip()
                if question.startswith('/addquiz'):
                    question = question[len('/addquiz'):].strip()

                options = [opt.strip() for opt in question_data['options']]

                # Convert correct_answer to zero-based index if needed
                correct_answer = question_data['correct_answer']
                if isinstance(correct_answer, str):
                    try:
                        correct_answer = int(correct_answer)
                    except ValueError:
                        logger.warning(f"Invalid correct_answer format: {correct_answer}")
                        stats['rejected']['invalid_format'] += 1
                        continue

                if isinstance(correct_answer, int) and correct_answer > 0:
                    correct_answer = correct_answer - 1

                # Validate question text
                if not question or len(question) < 5:
                    logger.warning(f"Question text too short: {question}")
                    stats['rejected']['invalid_format'] += 1
                    stats['errors'].append(f"Question text too short: {question}")
                    continue

                # Check for duplicates (only if allow_duplicates is False)
                if not allow_duplicates:
                    if any(q['question'].lower() == question.lower() for q in self.questions):
                        logger.warning(f"Duplicate question detected: {question}")
                        stats['rejected']['duplicates'] += 1
                        stats['errors'].append(f"Duplicate question: {question}")
                        continue

                # Validate options
                if len(options) != 4 or not all(opt for opt in options):
                    logger.warning(f"Invalid options for question: {question}")
                    stats['rejected']['invalid_options'] += 1
                    stats['errors'].append(f"Invalid options for question: {question}")
                    continue

                # Validate correct answer index
                if not isinstance(correct_answer, int) or not (0 <= correct_answer < 4):
                    logger.warning(f"Invalid correct answer index for question: {question}")
                    stats['rejected']['invalid_format'] += 1
                    stats['errors'].append(f"Invalid correct answer index for question: {question}")
                    continue

                # Add valid question
                question_obj = {
                    'question': question,
                    'options': options,
                    'correct_answer': correct_answer
                }
                added_questions.append(question_obj)
                stats['added'] += 1
                logger.info(f"Added question: {question}")

            except Exception as e:
                logger.error(f"Error processing question: {str(e)}\n{traceback.format_exc()}")
                stats['errors'].append(f"Unexpected error: {str(e)}")

        if stats['added'] > 0:
            # Save to database and update in-memory cache
            for question_obj in added_questions:
                try:
                    db_id = self.db.add_question(
                        question=question_obj['question'],
                        options=question_obj['options'],
                        correct_answer=question_obj['correct_answer']
                    )
                    if db_id:
                        # Add to in-memory cache with database ID
                        question_obj['id'] = db_id
                        self.questions.append(question_obj)
                        stats['db_saved'] += 1
                        logger.info(f"Saved question to database with ID {db_id}: {question_obj['question'][:50]}...")
                    else:
                        stats['db_failed'] += 1
                        logger.error(f"Failed to save question to database: {question_obj['question'][:50]}...")
                except Exception as e:
                    stats['db_failed'] += 1
                    logger.error(f"Database error saving question: {str(e)}\n{traceback.format_exc()}")
            
            logger.info(f"Added {stats['added']} questions. New total: {len(self.questions)}. DB saved: {stats['db_saved']}, DB failed: {stats['db_failed']}")

        return stats

    def edit_question(self, index: int, data: Dict):
        """Edit an existing question with validation.
        
        Validates question data and updates the question at the specified index.
        
        Args:
            index (int): Index of question to edit (0-based).
            data (Dict): Question data with 'question', 'options', 'correct_answer' keys.
        
        Raises:
            ValidationError: If index is out of range or question data is invalid.
            DatabaseError: If save fails.
        """
        if not (0 <= index < len(self.questions)):
            raise ValidationError(f"Question index {index} out of range (0-{len(self.questions)-1})")
        
        # Validate question data
        question = data.get('question', '').strip()
        if not question:
            raise ValidationError("Question text cannot be empty")
        
        # Validate options
        options = data.get('options', [])
        if not isinstance(options, list) or len(options) != 4:
            raise ValidationError("Must provide exactly 4 options")
        
        # Clean and validate options
        options = [opt.strip() for opt in options]
        if any(not opt for opt in options):
            raise ValidationError("All options must have text")
        
        # Check for duplicate options
        if len(set(options)) != len(options):
            raise ValidationError("Options must be unique")
        
        # Validate correct answer
        correct_answer = data.get('correct_answer')
        if not isinstance(correct_answer, int) or not (0 <= correct_answer < 4):
            raise ValidationError("Correct answer must be 0, 1, 2, or 3")
        
        # Update question in memory (note: DB update would need question ID)
        self.questions[index] = {
            'question': question,
            'options': options,
            'correct_answer': correct_answer
        }
        
        logger.info(f"Edited question {index}: {question[:50]}...")
    
    def delete_question(self, index: int):
        """Delete a question with validation.
        
        Args:
            index (int): Index of question to delete (0-based).
        
        Raises:
            ValidationError: If index is out of range.
            DatabaseError: If save fails.
        """
        if not (0 <= index < len(self.questions)):
            raise ValidationError(f"Question index {index} out of range (0-{len(self.questions)-1})")
        
        deleted = self.questions.pop(index)
        logger.info(f"Deleted question {index}: {deleted['question'][:50]}...")

    def get_all_questions(self) -> List[Dict]:
        """Get all questions from PostgreSQL database.
        
        Returns cached questions from memory. To refresh cache, call with force_reload.
        
        Returns:
            List[Dict]: List of all quiz questions from cache.
        """
        return self.questions


    def delete_question_by_db_id(self, db_id: int) -> bool:
        """Delete question by database ID from PostgreSQL only.
        
        Deletes from database and refreshes in-memory cache.
        
        Args:
            db_id: Database ID of question to delete
            
        Returns:
            bool: True if deleted successfully, False if not found
        """
        try:
            # Delete from database
            if not self.db.delete_question(db_id):
                logger.warning(f"Question ID {db_id} not found in database")
                return False
            
            # Remove from in-memory cache
            initial_count = len(self.questions)
            self.questions = [q for q in self.questions if q.get('id') != db_id]
            removed_count = initial_count - len(self.questions)
            
            logger.info(f"Deleted question {db_id} from database and cache ({removed_count} items removed from cache)")
            return True
                
        except Exception as e:
            logger.error(f"Error deleting question {db_id}: {e}")
            raise DatabaseError(f"Failed to delete question {db_id}: {e}") from e

    def edit_question_by_db_id(self, db_id: int, data: Dict) -> bool:
        """Edit question by database ID in PostgreSQL only.
        
        Updates question in database and refreshes in-memory cache.
        
        Args:
            db_id: Database ID of question to edit
            data: Question data with 'question', 'options', 'correct_answer' keys
            
        Returns:
            bool: True if edited successfully, False if not found
        
        Raises:
            ValidationError: If question data is invalid
            DatabaseError: If database update fails
        """
        try:
            # Validate question text
            question = data.get('question', '').strip()
            if not question or len(question) < 10:
                raise ValidationError("Question must be at least 10 characters long")
            
            # Validate options
            options = data.get('options', [])
            if not isinstance(options, list) or len(options) != 4:
                raise ValidationError("Must provide exactly 4 options")
            if not all(isinstance(opt, str) and opt.strip() for opt in options):
                raise ValidationError("All options must be non-empty strings")
            
            # Validate correct answer
            correct_answer = data.get('correct_answer')
            if not isinstance(correct_answer, int) or not (0 <= correct_answer < 4):
                raise ValidationError("Correct answer must be 0, 1, 2, or 3")
            
            # Update in database
            if not self.db.update_question(db_id, question, options, correct_answer):
                logger.warning(f"Question ID {db_id} not found in database")
                return False
            
            # Update in-memory cache
            for i, q in enumerate(self.questions):
                if q.get('id') == db_id:
                    self.questions[i] = {
                        'id': db_id,
                        'question': question,
                        'options': options,
                        'correct_answer': correct_answer
                    }
                    logger.info(f"Edited question {db_id} in database and cache: {question[:50]}...")
                    return True
            
            # If not in cache, reload from database
            logger.warning(f"Question {db_id} updated in DB but not found in cache, reloading...")
            db_questions = self.db.get_all_questions()
            self.questions = []
            for db_q in db_questions:
                self.questions.append({
                    'id': db_q['id'],
                    'question': db_q['question'],
                    'options': db_q['options'],
                    'correct_answer': db_q['correct_answer']
                })
            return True
                
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"Error editing question {db_id}: {e}")
            raise DatabaseError(f"Failed to edit question {db_id}: {e}") from e

    def get_quiz_stats(self) -> Dict:
        """Get comprehensive quiz statistics from PostgreSQL database.
        
        Returns detailed statistics including quiz counts, category breakdown,
        and integrity status. PostgreSQL-only implementation.
        
        Returns:
            Dict: Statistics with keys:
                - total_quizzes: Total number of questions (from database)
                - db_count: Database count (same as total for PostgreSQL-only)
                - categories: Category breakdown {category: count}
                - category_count: Number of unique categories
                - integrity_status: Always 'synced' (PostgreSQL-only)
                - difference: Always 0 (no dual storage)
        """
        try:
            # Get fresh count directly from database for accuracy
            db_questions = self.db.get_all_questions()
            total_count = len(db_questions)
            
            # Sync in-memory cache if count mismatch detected
            if len(self.questions) != total_count:
                logger.warning(f"Cache/DB mismatch detected! Cache: {len(self.questions)}, DB: {total_count}. Reloading cache...")
                self.questions = []
                for db_q in db_questions:
                    self.questions.append({
                        'id': db_q['id'],
                        'question': db_q['question'],
                        'options': db_q['options'],
                        'correct_answer': db_q['correct_answer']
                    })
                logger.info(f"Cache reloaded with {len(self.questions)} questions from database")
            
            # Get category breakdown
            categories = {}
            for question in db_questions:
                category = question.get('category', 'General')
                categories[category] = categories.get(category, 0) + 1
            
            return {
                'total_quizzes': total_count,
                'db_count': total_count,
                'categories': categories,
                'category_count': len(categories),
                'integrity_status': 'synced',
                'difference': 0
            }
        except Exception as e:
            logger.error(f"Error getting quiz stats: {e}")
            return {
                'total_quizzes': 0,
                'db_count': 0,
                'categories': {},
                'category_count': 0,
                'integrity_status': 'error',
                'difference': 0
            }

    def increment_score(self, user_id: int):
        """Increment user's score and synchronize with statistics.
        
        Updates both scores and stats dictionaries to keep them in sync.
        Records the attempt and saves data.
        
        Args:
            user_id (int): Telegram user ID.
        
        Raises:
            DatabaseError: If save fails.
        """
        user_id_str = str(user_id)
        if user_id_str not in self.stats:
            self._init_user_stats(user_id_str)

        # Initialize score if needed
        if user_id_str not in self.scores:
            self.scores[user_id_str] = 0

        # Increment score and synchronize with stats
        self.scores[user_id_str] += 1
        stats = self.stats[user_id_str]
        stats['correct_answers'] = self.scores[user_id_str]
        stats['total_quizzes'] = max(stats['total_quizzes'] + 1, stats['correct_answers'])

        # Record the attempt after synchronizing
        self.record_attempt(user_id, True)

    def get_score(self, user_id: int) -> int:
        """Get user's current score.
        
        Args:
            user_id (int): Telegram user ID.
        
        Returns:
            int: User's current score (0 if user not found).
        """
        return self.scores.get(str(user_id), 0)

    def add_active_chat(self, chat_id: int):
        """Add a chat to active chats.
        
        Initializes tracking structures for question history and saves changes.
        
        Args:
            chat_id (int): Telegram chat ID.
        
        Raises:
            DatabaseError: If save fails.
        """
        try:
            if chat_id not in self.active_chats:
                self.active_chats.append(chat_id)
                # Initialize tracking structures for new chat
                chat_id_str = str(chat_id)
                self.recent_questions[chat_id_str] = deque(maxlen=50)
                self.last_question_time[chat_id_str] = {}
                self._initialize_available_questions(chat_id)
                logger.info(f"Added chat {chat_id} to active chats with initialization")
        except Exception as e:
            logger.error(f"Error adding chat {chat_id}: {e}")

    def remove_active_chat(self, chat_id: int):
        """Remove a chat from active chats with cleanup"""
        try:
            chat_id_str = str(chat_id)
            if chat_id in self.active_chats:
                self.active_chats.remove(chat_id)

                # Cleanup chat data
                if chat_id_str in self.last_question_time:
                    del self.last_question_time[chat_id_str]
                if chat_id_str in self.recent_questions:
                    del self.recent_questions[chat_id_str]
                if chat_id_str in self.available_questions:
                    del self.available_questions[chat_id_str]

                logger.info(f"Removed chat {chat_id} from active chats with cleanup")
        except Exception as e:
            logger.error(f"Error removing chat {chat_id}: {e}")

    def get_active_chats(self) -> List[int]:
        return self.active_chats

    def cleanup_oldquestions(self) -> None:
        """Clean up old questions history and inactive chats"""
        try:
            current_date= datetime.now().strftime('%Y-%m-%d')
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            # Clean up old questions from inactive chats
            inactive_chats = []
            for chat_id in self.active_chats:
                chat_id_str = str(chat_id)
                last_activity = self.get_group_last_activity(chat_id_str)

                if not last_activity or last_activity < week_ago:
                    inactive_chats.append(chat_id)

                    # Clean up associated data
                    if chat_id_str in self.recent_questions:
                        del self.recent_questions[chat_id_str]
                    if chat_id_str in self.last_question_time:
                        del self.last_question_time[chat_id_str]
                    if chat_id_str in self.available_questions:
                        del self.available_questions[chat_id_str]

            # Remove inactive chats
            for chat_id in inactive_chats:
                if chat_id in self.active_chats:
                    self.active_chats.remove(chat_id)
                    logger.info(f"Removed inactive chat: {chat_id}")

            # Log cleanup
            if inactive_chats:
                logger.info(f"Cleaned up {len(inactive_chats)} inactive chats")

            # Clean up old daily activity data
            for user_id, stats in self.stats.items():
                try:
                    # Clean daily activity
                    old_dates = [
                        date for date in stats['daily_activity']
                        if date < week_ago
                    ]
                    for date in old_dates:
                        del stats['daily_activity'][date]

                    # Clean group activity
                    for group_id, group_stats in stats.get('groups', {}).items():
                        old_group_dates = [
                            date for date in group_stats.get('daily_activity', {})
                            if date < week_ago
                        ]
                        for date in old_group_dates:
                            del group_stats['daily_activity'][date]

                except Exception as e:
                    logger.error(f"Error cleaning up stats for user {user_id}: {e}")
                    continue

            logger.info("Completed cleanup of old questions and inactive chats")

        except Exception as e:
            logger.error(f"Error in cleanup_old_questions: {e}")

    def validate_question(self, question: Dict) -> bool:
        """Validate if a question's format and answer are correct.
        
        Checks that question has all required fields, 4 options, and a valid
        correct_answer index.
        
        Args:
            question (Dict): Question dictionary to validate
        
        Returns:
            bool: True if question is valid, False otherwise
        """
        try:
            # Basic structure validation
            if not all(key in question for key in ['question', 'options', 'correct_answer']):
                return False

            # Validate options array
            if not isinstance(question['options'], list) or len(question['options']) != 4:
                return False

            # Validate correct_answer is within bounds
            if not isinstance(question['correct_answer'], int) or not (0 <= question['correct_answer'] < 4):
                return False

            return True
        except Exception:
            return False

    def remove_invalidquestions(self):
        """Remove questions with invalid format or answers.
        
        Validates all questions and removes any that don't meet requirements.
        Automatically saves the cleaned question list.
        
        Returns:
            Dict: Statistics about the cleanup with keys:
                - initial_count: Number of questions before cleanup
                - removed_count: Number of questions removed
                - remaining_count: Number of valid questions remaining
        
        Raises:
            DatabaseError: If validation or save fails
        """
        try:
            initial_count = len(self.questions)
            self.questions = [q for q in self.questions if self.validate_question(q)]
            removed_count = initial_count - len(self.questions)

            logger.info(f"Removed {removed_count} invalid questions. Remaining: {len(self.questions)}")
            return {
                'initial_count': initial_count,
                'removed_count': removed_count,
                'remaining_count': len(self.questions)
            }
        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Failed to remove invalid questions: {e}")
            raise DatabaseError(f"Failed to remove invalid questions: {e}") from e

    def clear_all_questions(self) -> bool:
        """Clear all questions from the database.
        
        Removes all quiz questions and saves immediately.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.questions = []
            logger.info("All questions cleared from cache")
            return True
        except Exception as e:
            logger.error(f"Error clearing questions: {e}")
            return False

    def reload_data(self):
        """Reload questions from database and refresh cache.
        
        Reloads questions from PostgreSQL database and refreshes in-memory cache.
        
        Returns:
            bool: True if reload successful
        
        Raises:
            DatabaseError: If reload fails
        """
        try:
            logger.info("Reloading questions from database...")

            # Reset caches and tracking structures
            self._cached_questions = None
            self._cached_leaderboard = None
            self._leaderboard_cache_time = None
            self.recent_questions.clear()
            self.last_question_time.clear()
            self.available_questions.clear()

            # Reload questions from database
            db_questions = self.db.get_all_questions()
            self.questions = []
            for db_q in db_questions:
                self.questions.append({
                    'id': db_q['id'],
                    'question': db_q['question'],
                    'options': db_q['options'],
                    'correct_answer': db_q['correct_answer']
                })

            # Log detailed results
            logger.info("Data reload completed successfully:")
            logger.info(f"- Questions loaded: {len(self.questions)}")
            return True

        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Failed to reload quiz data: {str(e)}\n{traceback.format_exc()}")
            raise DatabaseError(f"Failed to reload data: {e}") from e

    def get_group_last_activity(self, chat_id: str) -> Optional[str]:
        """Get the last activity date for a group.
        
        Finds the most recent activity date across all users in the group.
        
        Args:
            chat_id (str): Chat ID as string
        
        Returns:
            Optional[str]: Most recent activity date in YYYY-MM-DD format,
                          None if no activity found
        """
        try:
            latest_activity = None
            chat_id_str = str(chat_id)

            # Check all users' group activity
            for stats in self.stats.values():
                if chat_id_str in stats.get('groups', {}):
                    group_last_activity = stats['groups'][chat_id_str].get('last_activity_date')
                    if group_last_activity:
                        if not latest_activity or group_last_activity > latest_activity:
                            latest_activity = group_last_activity

            return latest_activity
        except Exception as e:
            logger.error(f"Error getting group last activity: {e}")
            return None

    def get_global_statistics(self) -> Dict:
        """Get comprehensive global statistics with accurate user counting.
        
        Calculates and returns bot-wide statistics including user counts,
        group activity, quiz performance, and success rates.
        
        Returns:
            Dict: Global statistics with nested dictionaries:
                - users: Total, active (today/week), private/group breakdown
                - groups: Total, active (today/week)
                - quizzes: Total attempts, correct answers, activity metrics
                - performance: Success rate, available questions
        """
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            # Initialize stats structure
            stats = {
                'users': {
                    'total': 0,
                    'active_today': 0,
                    'active_week': 0,
                    'private_chat': 0,
                    'group_users': 0
                },
                'groups': {
                    'total': len(self.active_chats),
                    'active_today': 0,
                    'active_week': 0
                },
                'quizzes': {
                    'total_attempts': 0,
                    'correct_answers': 0,
                    'today_attempts': 0,
                    'week_attempts': 0
                },
                'performance': {
                    'success_rate': 0.0,
                    'questions_available': len(self.questions)
                }
            }

            # Get all group members
            group_users = set()
            private_users = set()
            for chat_id in self.active_chats:
                members = self.get_group_members(str(chat_id))
                group_users.update(members)

            # Process user statistics
            for user_id, user_stats in self.stats.items():
                # Track private chat users
                if 'private_chat_activity' in user_stats and user_stats['private_chat_activity'].get('total_messages', 0) > 0:
                    private_users.add(user_id)
                    stats['users']['private_chat'] += 1

                # Track activity periods
                last_active = user_stats.get('last_activity_date')
                if last_active:
                    if last_active == current_date:
                        stats['users']['active_today'] += 1
                    if last_active >= week_start:
                        stats['users']['active_week'] += 1

                # Track quiz performance
                stats['quizzes']['total_attempts'] += user_stats.get('total_quizzes', 0)
                stats['quizzes']['correct_answers'] += user_stats.get('correct_answers', 0)

                # Track today's attempts
                today_activity = user_stats.get('daily_activity', {}).get(current_date, {})
                stats['quizzes']['today_attempts'] += today_activity.get('attempts', 0)

                # Track week's attempts
                stats['quizzes']['week_attempts'] += sum(
                    day_stats.get('attempts', 0)
                    for date, day_stats in user_stats.get('daily_activity', {}).items()
                    if date >= week_start
                )

            # Update group activity
            for chat_id in self.active_chats:
                chat_id_str = str(chat_id)
                last_activity = self.get_group_last_activity(chat_id_str)
                if last_activity:
                    if last_activity == current_date:
                        stats['groups']['active_today'] += 1
                    if last_activity >= week_start:
                        stats['groups']['active_week'] += 1

            # Calculate final user counts
            all_users = group_users.union(private_users)
            stats['users']['total'] = len(all_users)
            stats['users']['group_users'] = len(group_users)

            # Calculate success rate
            if stats['quizzes']['total_attempts'] > 0:
                stats['performance']['success_rate'] = round(
                    float(stats['quizzes']['correct_answers']) / float(stats['quizzes']['total_attempts']) * 100.0, 
                    1
                )

            logger.info(f"Global stats generated: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error getting global statistics: {e}\n{traceback.format_exc()}")
            return {
                'users': {'total': 0, 'active_today': 0, 'active_week': 0, 'private_chat': 0, 'group_users': 0},
                'groups': {'total': 0, 'active_today': 0, 'active_week': 0},
                'quizzes': {'total_attempts': 0, 'correct_answers': 0, 'today_attempts': 0, 'week_attempts': 0},
                'performance': {'success_rate': 0, 'questions_available': 0}
            }

    def get_group_members(self, chat_id: str) -> set:
        """Get all members who have participated in a group.
        
        Args:
            chat_id (str): Chat ID as string
        
        Returns:
            set: Set of user ID strings who have participated in the group
        """
        members = set()
        for user_id, stats in self.stats.items():
            if 'groups' in stats and chat_id in stats['groups']:
                members.add(user_id)
        return members

    def track_user_activity(self, user_id: int, chat_id: int) -> None:
        """Track user activity in real-time.
        
        Updates last activity timestamps for users and initializes group
        tracking if needed. Automatically saves data.
        
        Args:
            user_id (int): Telegram user ID
            chat_id (int): Telegram chat ID
        """
        try:
            user_id_str = str(user_id)
            chat_id_str = str(chat_id)
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Initialize user if not exists
            if user_id_str not in self.stats:
                self._init_user_stats(user_id_str)

            # Update user's last activity
            self.stats[user_id_str]['last_activity_date'] = current_date

            # Update group activity if it's a group chat
            if chat_id_str not in self.stats[user_id_str].get('groups', {}):
                self.stats[user_id_str]['groups'][chat_id_str] = {
                    'total_quizzes': 0,
                    'correct_answers': 0,
                    'score': 0,
                    'last_activity_date': current_date,
                    'daily_activity': {},
                    'current_streak': 0,
                    'longest_streak': 0,
                    'last_correct_date': None
                }

            # Activity tracked in memory
            logger.info(f"Tracked activity for user {user_id} in chat {chat_id}")

        except Exception as e:
            logger.error(f"Error tracking user activity: {e}")

    def get_active_users(self) -> List[str]:
        """Get list of active users with improved tracking.
        
        Returns users who have been active within the last 7 days based on
        any activity type (private chat, group participation, etc.).
        
        Returns:
            List[str]: List of active user ID strings
        """
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            active_users = set()

            # Check all activity types
            for user_id, stats in self.stats.items():
                # Check last activity date
                last_activity = stats.get('last_activity_date')
                if last_activity and last_activity >= week_start:
                    active_users.add(user_id)
                    continue

                # Check private chat activity
                private_chat = stats.get('private_chat_activity', {})
                if private_chat.get('last_active', '') >= week_start:
                    active_users.add(user_id)
                    continue

                # Check group activity
                for group_stats in stats.get('groups', {}).values():
                    if group_stats.get('last_activity_date', '') >= week_start:
                        active_users.add(user_id)
                        break

            return list(active_users)
        except Exception as e:
            logger.error(f"Error getting active users: {e}")
            return []

    def update_all_stats(self) -> None:
        """Update all statistics in real-time with enhanced tracking.
        
        Ensures all user statistics have required fields, syncs with scores,
        updates daily activity tracking, and cleans up old data.
        Automatically saves after updates.
        """
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            # Update user stats
            for user_id, stats in self.stats.items():
                try:
                    # Ensure required fields exist
                    if 'join_date' not in stats:
                        stats['join_date'] = current_date
                    if 'last_activity_date' not in stats:
                        stats['last_activity_date'] = current_date
                    if 'private_chat_activity' not in stats:
                        stats['private_chat_activity'] = {
                            'total_messages': 0,
                            'last_active': current_date
                        }

                    # Ensure daily activity exists
                    if current_date not in stats['daily_activity']:
                        stats['daily_activity'][current_date] = {
                            'attempts': 0,
                            'correct': 0
                        }

                    # Update group stats
                    for group_id, group_stats in stats.get('groups', {}).items():
                        if current_date not in group_stats.get('daily_activity', {}):
                            group_stats['daily_activity'][current_date] = {
                                'attempts': 0,
                                'correct': 0
                            }

                        # Clean up old daily activity data
                        old_dates = [
                            date for date in group_stats['daily_activity']
                            if date < week_start
                        ]
                        for date in old_dates:
                            del group_stats['daily_activity'][date]

                    # Sync with scores
                    score = self.scores.get(user_id, 0)
                    if score != stats['correct_answers']:
                        stats['correct_answers'] = score
                        stats['total_quizzes'] = max(stats['total_quizzes'], score)

                except Exception as e:
                    logger.error(f"Error updating stats for user {user_id}: {e}")
                    continue

            # Stats updated in memory
            logger.info("All stats updated successfully")

        except Exception as e:
            logger.error(f"Error updating all stats: {e}")

    def cleanup_old_questions(self):
        """Cleanup old question history periodically.
        
        Removes question tracking data older than 24 hours to prevent
        memory buildup while maintaining recent question avoidance.
        """
        try:
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(hours=24)

            for chat_id in list(self.recent_questions.keys()):
                # Clear tracking for inactive chats
                if not self.recent_questions[chat_id]:
                    del self.recent_questions[chat_id]
                    if chat_id in self.last_question_time:
                        del self.last_question_time[chat_id]
                    if chat_id in self.available_questions:
                        del self.available_questions[chat_id]
                    continue

                # Remove old question timestamps
                if chat_id in self.last_question_time:
                    old_questions = [
                        q for q, t in self.last_question_time[chat_id].items()
                        if t < cutoff_time
                    ]
                    for q in old_questions:
                        del self.last_question_time[chat_id][q]

            logger.info("Completed cleanup of old questions history")
        except Exception as e:
            logger.error(f"Error in cleanup_old_questions: {e}")