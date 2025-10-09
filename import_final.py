#!/usr/bin/env python3
"""
Final MCQ Import Script - Handles duplicates gracefully
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

def extract_answer_key(text_lines):
    """Extract answer key from document"""
    answer_dict = {}
    
    for line in text_lines:
        matches = re.findall(r'(\d+)\.\(([A-D])\)', line)
        for match in matches:
            question_num = int(match[0])
            answer_letter = match[1]
            answer_dict[question_num] = answer_letter
    
    print(f"✅ Extracted {len(answer_dict)} answers from answer key")
    return answer_dict

def parse_mcqs_with_answers(text_lines, answer_dict):
    """Parse MCQs and match with answer key"""
    questions = []
    current_question = None
    current_options = []
    question_number = 0
    seen_questions = set()  # Track duplicates
    
    i = 0
    while i < len(text_lines):
        line = text_lines[i].strip()
        
        question_match = re.match(r'^(\d+)\.\s+(.+)', line)
        
        if question_match:
            # Save previous question if it exists
            if current_question and len(current_options) >= 4:
                # Check for duplicate
                q_lower = current_question.lower()
                if q_lower not in seen_questions:
                    seen_questions.add(q_lower)
                    
                    answer_letter = answer_dict.get(question_number, 'A')
                    answer_index = ord(answer_letter) - ord('A')
                    
                    questions.append({
                        'number': question_number,
                        'question': current_question,
                        'options': current_options[:4],
                        'answer_letter': answer_letter,
                        'answer_index': answer_index
                    })
            
            # Start new question
            question_number = int(question_match.group(1))
            current_question = question_match.group(2).strip()
            current_options = []
        
        elif line and current_question and not question_match:
            if line.lower() not in ['discussion', 'explanation', 'answers :', '']:
                if not re.search(r'\d+\.\([A-D]\)', line):
                    option_text = re.sub(r'^\([A-D]\)\s*', '', line)
                    if option_text and len(current_options) < 4:
                        current_options.append(option_text)
        
        i += 1
    
    # Last question
    if current_question and len(current_options) >= 4:
        q_lower = current_question.lower()
        if q_lower not in seen_questions:
            answer_letter = answer_dict.get(question_number, 'A')
            answer_index = ord(answer_letter) - ord('A')
            questions.append({
                'number': question_number,
                'question': current_question,
                'options': current_options[:4],
                'answer_letter': answer_letter,
                'answer_index': answer_index
            })
    
    with_answers = sum(1 for q in questions if q['number'] in answer_dict)
    print(f"✅ Parsed {len(questions)} unique MCQs (duplicates removed)")
    print(f"   📝 {with_answers} questions have answers from answer key")
    
    return questions

def import_to_database(questions):
    """Import questions into PostgreSQL database"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False  # Manual transaction control
        cursor = conn.cursor()
        
        print(f"\n🗄️  Connected to database")
        
        # Clear existing questions
        cursor.execute("DELETE FROM questions")
        conn.commit()
        print(f"🗑️  Cleared existing questions")
        
        # Reset sequence
        cursor.execute("ALTER SEQUENCE questions_id_seq RESTART WITH 1")
        conn.commit()
        print(f"🔄 Reset question ID sequence")
        
        imported = 0
        skipped = 0
        
        for q in questions:
            try:
                # Start new transaction for each question
                cursor.execute('''
                    INSERT INTO questions (question, options, correct_answer, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (
                    q['question'],
                    str(q['options']),
                    q['answer_index'],
                    datetime.now(),
                    datetime.now()
                ))
                conn.commit()
                imported += 1
                
                if imported % 50 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                
            except psycopg2.IntegrityError as e:
                conn.rollback()
                skipped += 1
                print(f"  ⏭️  Skipped duplicate question {q['number']}")
            except Exception as e:
                conn.rollback()
                print(f"❌ Error importing question {q['number']}: {e}")
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM questions")
        final_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Import Complete!")
        print(f"   ✅ Successfully imported: {imported} questions")
        print(f"   ⏭️  Skipped duplicates: {skipped}")
        print(f"   📊 Total in database: {final_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def main():
    """Main execution function"""
    print("=" * 60)
    print("📚 Final MCQ Import - With Duplicate Handling")
    print("=" * 60)
    print()
    
    docx_path = "attached_assets/5_6284844636582190726_1760022331786.docx"
    
    if not os.path.exists(docx_path):
        print(f"❌ File not found: {docx_path}")
        return
    
    # Extract text
    text_lines = extract_text_from_docx(docx_path)
    
    # Extract answer key
    answer_dict = extract_answer_key(text_lines)
    
    # Parse MCQs with answers
    questions = parse_mcqs_with_answers(text_lines, answer_dict)
    
    if not questions:
        print("❌ No questions found in document")
        return
    
    # Show sample
    print(f"\n📝 Sample Questions:")
    print(f"   Q{questions[0]['number']}: {questions[0]['question'][:50]}... → Answer: {questions[0]['answer_letter']}")
    print(f"   Q{questions[50]['number']}: {questions[50]['question'][:50]}... → Answer: {questions[50]['answer_letter']}")
    print(f"   Q{questions[-1]['number']}: {questions[-1]['question'][:50]}... → Answer: {questions[-1]['answer_letter']}")
    
    # Import to database
    import_to_database(questions)

if __name__ == "__main__":
    main()
