import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

def initialize_firebase():
    """Initialize Firebase Admin SDK"""
    try:
        # Check if Firebase is already initialized
        if not firebase_admin._apps:
            # Initialize Firebase with service account key file
            cred = credentials.Certificate('service_account_key.json')
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        print("Make sure to update service_account_key.json with your actual Firebase credentials")
        return None

def get_file_from_database(db, file_name):
    """Retrieve a specific file from the Firebase database"""
    if not db:
        print("Firestore client not initialized")
        return None
    
    try:
        # Query the DNA collection for the specified file
        collection_ref = db.collection('DNA')
        query = collection_ref.where('file_name', '==', file_name)
        docs = query.stream()
        
        for doc in docs:
            doc_data = doc.to_dict()
            print(f"Found file: {doc_data['file_name']}")
            print(f"Sequences count: {doc_data['sequences_count']}")
            print(f"Contains target sequence: {doc_data['contains_target_sequence']}")
            return doc_data
        
        print(f"File '{file_name}' not found in database")
        return None
        
    except Exception as e:
        print(f"Error retrieving file from database: {e}")
        return None

def create_txt_file(file_data, output_filename=None):
    """Create a txt file with DNA sequences as a continuous string without line breaks"""
    if not file_data:
        print("No file data provided")
        return
    
    # Generate output filename if not provided
    if not output_filename:
        base_name = file_data['file_name'].replace('.jsonl', '')
        output_filename = f"{base_name}_sequences.txt"
    
    try:
        # Create output directory if it doesn't exist
        output_dir = "extracted_sequences"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)
        
        with open(output_path, 'w') as f:
            # Write sequences as continuous string without line breaks
            for seq_data in file_data['sequences']:
                f.write(seq_data['sequence'])
        
        print(f"Successfully created {output_path} with {len(file_data['sequences'])} DNA sequences")
        return output_path
        
    except Exception as e:
        print(f"Error creating txt file: {e}")
        return None

def list_available_files(db):
    """List all available files in the database"""
    if not db:
        print("Firestore client not initialized")
        return []
    
    try:
        collection_ref = db.collection('DNA')
        docs = collection_ref.stream()
        
        files = []
        print("\nAvailable files in database:")
        print("-" * 40)
        
        for doc in docs:
            doc_data = doc.to_dict()
            file_name = doc_data['file_name']
            seq_count = doc_data['sequences_count']
            has_target = doc_data['contains_target_sequence']
            
            files.append(file_name)
            print(f"File: {file_name}")
            print(f"  Sequences: {seq_count}")
            print(f"  Has target: {has_target}")
            print()
        
        return files
        
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def main():
    """Main function to retrieve file and create txt output"""
    print("DNA Sequence Retriever")
    print("=" * 30)
    
    # Initialize Firebase
    db = initialize_firebase()
    if not db:
        return
    
    # List available files
    available_files = list_available_files(db)
    
    if not available_files:
        print("No files found in database")
        return
    
    # Get user input for file selection
    print(f"\nFound {len(available_files)} files in database")
    file_name = input("Enter the exact file name to retrieve (e.g., dna_sequences_1.jsonl): ").strip()
    
    if not file_name:
        print("No file name provided")
        return
    
    # Retrieve file from database
    print(f"\nRetrieving {file_name} from database...")
    file_data = get_file_from_database(db, file_name)
    
    if file_data:
        # Create txt file with sequences
        print("\nCreating txt file with DNA sequences...")
        output_path = create_txt_file(file_data)
        
        if output_path:
            print(f"\nProcess completed successfully!")
            print(f"Output file: {output_path}")
        else:
            print("Failed to create output file")
    else:
        print("Failed to retrieve file from database")

if __name__ == "__main__":
    main()
