#!/usr/bin/env python3
"""
Fix options format in database - Convert Python strings to proper JSON
"""

import os
import json
import ast
import psycopg2

def fix_options():
    """Convert all options from Python string format to JSON format"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("🗄️  Connected to database")
        
        # Get all questions
        cursor.execute("SELECT id, options FROM questions")
        questions = cursor.fetchall()
        
        print(f"📊 Found {len(questions)} questions to fix")
        
        fixed = 0
        for row in questions:
            question_id, options_str = row
            
            try:
                # Parse Python string to list
                options_list = ast.literal_eval(options_str)
                
                # Convert to proper JSON string
                json_options = json.dumps(options_list)
                
                # Update in database
                cursor.execute(
                    "UPDATE questions SET options = %s WHERE id = %s",
                    (json_options, question_id)
                )
                fixed += 1
                
                if fixed % 50 == 0:
                    print(f"  ⏳ Fixed {fixed} questions...")
                
            except Exception as e:
                print(f"❌ Error fixing question {question_id}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n✅ Fixed {fixed} questions!")
        print(f"   Options are now in proper JSON format")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 Fixing Options Format - Python String → JSON")
    print("=" * 60)
    print()
    fix_options()
