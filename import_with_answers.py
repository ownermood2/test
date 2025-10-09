#!/usr/bin/env python3
"""
Import MCQs with correct answers from Word Document
Extracts questions, options, AND answers from DOCX file with answer key
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
        # Match patterns like: 151.(C) 152.(B) 153.(D)
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
    
    i = 0
    while i < len(text_lines):
        line = text_lines[i].strip()
        
        # Check if it's a new question (starts with number followed by dot)
        question_match = re.match(r'^(\d+)\.\s+(.+)', line)
        
        if question_match:
            # Save previous question if it exists
            if current_question and len(current_options) >= 4:
                # Get answer from answer key
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
        
        # Collect options
        elif line and current_question and not question_match:
            # Skip special markers
            if line.lower() not in ['discussion', 'explanation', 'answers :', '']:
                # Skip if it looks like an answer key line
                if not re.search(r'\d+\.\([A-D]\)', line):
                    # Remove option markers
                    option_text = re.sub(r'^\([A-D]\)\s*', '', line)
                    if option_text and len(current_options) < 4:
                        current_options.append(option_text)
        
        i += 1
    
    # Don't forget the last question
    if current_question and len(current_options) >= 4:
        answer_letter = answer_dict.get(question_number, 'A')
        answer_index = ord(answer_letter) - ord('A')
        questions.append({
            'number': question_number,
            'question': current_question,
            'options': current_options[:4],
            'answer_letter': answer_letter,
            'answer_index': answer_index
        })
    
    # Statistics
    with_answers = sum(1 for q in questions if q['number'] in answer_dict)
    print(f"✅ Parsed {len(questions)} MCQs")
    print(f"   📝 {with_answers} questions have answers from answer key")
    print(f"   ⚠️  {len(questions) - with_answers} questions default to option A")
    
    return questions

def import_to_database(questions):
    """Import questions into PostgreSQL database"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print(f"\n🗄️  Connected to database")
        
        # Clear existing questions
        cursor.execute("DELETE FROM questions")
        print(f"🗑️  Cleared existing questions")
        
        # Reset sequence
        cursor.execute("ALTER SEQUENCE questions_id_seq RESTART WITH 1")
        print(f"🔄 Reset question ID sequence")
        
        imported = 0
        
        for q in questions:
            try:
                # Insert question
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
                
                imported += 1
                
                if imported % 50 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                
            except Exception as e:
                print(f"❌ Error importing question {q['number']}: {e}")
                continue
        
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM questions")
        final_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Import Complete!")
        print(f"   ✅ Successfully imported: {imported} questions")
        print(f"   📊 Total in database: {final_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def show_samples(questions):
    """Show sample questions with answers"""
    print(f"\n📝 Sample Questions with Answers:")
    print("=" * 60)
    
    samples = [questions[0], questions[len(questions)//2], questions[-1]]
    
    for q in samples:
        print(f"\nQ{q['number']}: {q['question'][:60]}...")
        for idx, opt in enumerate(q['options']):
            marker = "✓" if idx == q['answer_index'] else " "
            print(f"   [{marker}] {chr(65+idx)}. {opt[:50]}...")
        print(f"   Correct Answer: {q['answer_letter']}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("📚 MCQ Import Tool - With Answer Key")
    print("=" * 60)
    print()
    
    docx_path = "attached_assets/5_6284844636582190726_1760022331786.docx"
    
    if not os.path.exists(docx_path):
        print(f"❌ File not found: {docx_path}")
        return
    
    # Step 1: Extract text from DOCX
    text_lines = extract_text_from_docx(docx_path)
    
    # Step 2: Extract answer key
    answer_dict = extract_answer_key(text_lines)
    
    # Step 3: Parse MCQs with answers
    questions = parse_mcqs_with_answers(text_lines, answer_dict)
    
    if not questions:
        print("❌ No questions found in document")
        return
    
    # Step 4: Show samples
    show_samples(questions)
    
    # Step 5: Import to database
    print()
    import_to_database(questions)

if __name__ == "__main__":
    main()
