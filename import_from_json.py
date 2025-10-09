#!/usr/bin/env python3
"""
Import questions from JSON file
Format: [{question: str, options: [str, str, str, str], correct_answer: int}]
"""

import os
import json
import psycopg2
import sys

def import_from_json(json_file):
    """Import questions from JSON file"""
    
    # Read JSON file
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            questions = json.load(f)
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return False
    
    print(f"📖 Loaded {len(questions)} questions from JSON")
    
    # Validate format
    for i, q in enumerate(questions):
        if 'question' not in q or 'options' not in q or 'correct_answer' not in q:
            print(f"❌ Invalid format at question {i+1}: missing required fields")
            return False
        
        if len(q['options']) != 4:
            print(f"❌ Invalid format at question {i+1}: must have exactly 4 options")
            return False
        
        if not isinstance(q['correct_answer'], int) or q['correct_answer'] < 0 or q['correct_answer'] > 3:
            print(f"❌ Invalid format at question {i+1}: correct_answer must be 0, 1, 2, or 3")
            return False
    
    print("✅ JSON format validated")
    
    # Connect to database
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        before = cursor.fetchone()[0]
        print(f"\n📊 Current questions in database: {before}")
        
        imported = 0
        skipped = 0
        
        for q in questions:
            # Convert options to JSON string
            options_json = json.dumps(q['options'])
            
            try:
                cursor.execute(
                    "INSERT INTO questions (question, options, correct_answer) VALUES (%s, %s, %s)",
                    (q['question'], options_json, q['correct_answer'])
                )
                imported += 1
                
                if imported % 100 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                    
            except psycopg2.IntegrityError:
                # Duplicate question
                skipped += 1
                conn.rollback()
                continue
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        after = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Successfully imported {imported} new questions!")
        print(f"⚠️  Skipped {skipped} duplicates")
        print(f"📊 Total questions in database: {before} → {after}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_from_json.py <json_file>")
        print("\nExample:")
        print("  python import_from_json.py questions.json")
        sys.exit(1)
    
    json_file = sys.argv[1]
    
    if not os.path.exists(json_file):
        print(f"❌ File not found: {json_file}")
        sys.exit(1)
    
    print("=" * 70)
    print("📚 Importing Questions from JSON")
    print("=" * 70)
    print()
    
    success = import_from_json(json_file)
    
    if success:
        print("\n✅ Import completed successfully!")
    else:
        print("\n❌ Import failed!")
        sys.exit(1)
