#!/usr/bin/env python3
"""
Import MCQs from Word Document into PostgreSQL Database
Extracts questions, options, and answers from DOCX file
"""

import re
import os
import psycopg2
from docx import Document
from datetime import datetime

def extract_text_from_docx(docx_path):
    """Extract all text from DOCX file"""
    print(f"📂 Opening Word document: {docx_path}")
    doc = Document(docx_path)
    text_content = []
    
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text:
            text_content.append(text)
    
    print(f"✅ Extracted {len(text_content)} lines from document")
    return text_content

def parse_mcqs(text_lines):
    """Parse MCQs from extracted text"""
    questions = []
    current_question = None
    current_options = []
    answer = None
    question_number = 0
    
    i = 0
    while i < len(text_lines):
        line = text_lines[i].strip()
        
        # Check if it's a new question (starts with number followed by dot)
        question_match = re.match(r'^(\d+)\.\s+(.+)', line)
        
        if question_match:
            # Save previous question if it exists
            if current_question and len(current_options) >= 4:
                questions.append({
                    'number': question_number,
                    'question': current_question,
                    'options': current_options[:4],  # Take only first 4 options
                    'answer': answer
                })
            
            # Start new question
            question_number = int(question_match.group(1))
            current_question = question_match.group(2).strip()
            current_options = []
            answer = None
            
        # Check for answer patterns
        elif re.match(r'^(See Answer|Answer):\s*(.+)', line, re.IGNORECASE):
            answer_match = re.match(r'^(See Answer|Answer):\s*(.+)', line, re.IGNORECASE)
            if answer_match:
                answer = answer_match.group(2).strip()
        
        # Check if line is an option (not a question, not an answer)
        elif line and current_question and not question_match:
            # Skip if it's Discussion, Explanation, etc.
            if line.lower() not in ['discussion', 'explanation', '']:
                # Remove option markers like (A), (B), etc.
                option_text = re.sub(r'^\([A-D]\)\s*', '', line)
                if option_text and len(current_options) < 4:
                    current_options.append(option_text)
        
        i += 1
    
    # Don't forget the last question
    if current_question and len(current_options) >= 4:
        questions.append({
            'number': question_number,
            'question': current_question,
            'options': current_options[:4],
            'answer': answer
        })
    
    print(f"✅ Parsed {len(questions)} MCQs from document")
    return questions

def determine_correct_answer_index(options, answer_text):
    """Determine which option index is correct based on answer text"""
    if not answer_text:
        return 0  # Default to first option if no answer
    
    answer_lower = answer_text.lower().strip()
    
    # Check if answer is a single letter (a, b, c, d)
    if answer_lower in ['a', 'b', 'c', 'd']:
        return ord(answer_lower) - ord('a')
    
    # Check if answer matches any option text (case-insensitive)
    for idx, option in enumerate(options):
        if answer_lower in option.lower() or option.lower() in answer_lower:
            return idx
    
    # Check for partial matches
    for idx, option in enumerate(options):
        option_words = set(option.lower().split())
        answer_words = set(answer_lower.split())
        # If more than 50% of answer words match option
        if len(option_words & answer_words) / max(len(answer_words), 1) > 0.5:
            return idx
    
    print(f"⚠️  Could not match answer '{answer_text}' to any option")
    return 0  # Default to first option

def import_to_database(questions):
    """Import questions into PostgreSQL database"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print(f"🗄️  Connected to database")
        
        # Get current count
        cursor.execute("SELECT COUNT(*) FROM questions")
        before_count = cursor.fetchone()[0]
        print(f"📊 Current questions in database: {before_count}")
        
        imported = 0
        skipped = 0
        
        for q in questions:
            try:
                # Prepare data
                question_text = q['question']
                options = q['options']
                answer_text = q['answer'] if q['answer'] else ''
                correct_index = determine_correct_answer_index(options, answer_text)
                
                # Check for duplicate (case-insensitive)
                cursor.execute(
                    "SELECT id FROM questions WHERE LOWER(question) = LOWER(%s)",
                    (question_text,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    skipped += 1
                    continue
                
                # Insert question
                cursor.execute('''
                    INSERT INTO questions (question, options, correct_answer, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (
                    question_text,
                    str(options),
                    correct_index,
                    datetime.now(),
                    datetime.now()
                ))
                
                imported += 1
                
                if imported % 50 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                
            except Exception as e:
                print(f"❌ Error importing question {q['number']}: {e}")
                continue
        
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM questions")
        after_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Import Complete!")
        print(f"   📊 Before: {before_count} questions")
        print(f"   ➕ Imported: {imported} new questions")
        print(f"   ⏭️  Skipped: {skipped} duplicates")
        print(f"   📊 After: {after_count} questions")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def main():
    """Main execution function"""
    print("=" * 60)
    print("📚 MCQ Import Tool - Word Document to PostgreSQL")
    print("=" * 60)
    print()
    
    docx_path = "attached_assets/5_6284844636582190726_1760022331786.docx"
    
    if not os.path.exists(docx_path):
        print(f"❌ File not found: {docx_path}")
        return
    
    # Step 1: Extract text from DOCX
    text_lines = extract_text_from_docx(docx_path)
    
    # Step 2: Parse MCQs
    questions = parse_mcqs(text_lines)
    
    if not questions:
        print("❌ No questions found in document")
        return
    
    # Show sample
    print(f"\n📝 Sample Question:")
    sample = questions[0]
    print(f"   Q: {sample['question'][:80]}...")
    print(f"   Options: {len(sample['options'])}")
    print(f"   Answer: {sample['answer'] if sample['answer'] else 'Not specified'}")
    print()
    
    # Step 3: Import to database
    import_to_database(questions)

if __name__ == "__main__":
    main()
