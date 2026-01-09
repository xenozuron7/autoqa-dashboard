from dotenv import load_dotenv  # Load .env file
load_dotenv()  # Load environment variables
from flask import Flask, render_template, jsonify, request  # Web framework stuff
from pymongo import MongoClient  # MongoDB driver
from datetime import datetime, timedelta  # Date/time tools
import os  # System utilities

app = Flask(__name__)  # Create app
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-fallback') #Secret key

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')  # Get URI from environment variable

try:  # Connect to database
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)  # 5 sec timeout
    client.admin.command('ping')  # Test connection
    print("✓ Successfully connected to MongoDB!")  # Success
    
    db = client['AutoQA']  # Database (change if needed)
    collection = db['auto_qa_results_demo']  # Collection (change if needed)
    
    count = collection.count_documents({})  # Check document count
    print(f"✓ Found {count} documents in the collection")  # Show count
    
except Exception as e:  # Connection failed
    print(f"✗ Failed to connect to MongoDB: {e}")  # Error message
    client = None  # No connection

def parse_ticket_status(ticket):  # Figure out ticket status
    """Determine ticket status from auto_qa_status"""
    auto_qa_status = str(ticket.get('auto_qa_status', '')).lower()  # Get status field
    request_status = str(ticket.get('request_status', '')).lower()  # Backup status
    
    if auto_qa_status == 'complete':  # Done
        return 'completed'
    elif auto_qa_status == 'callback':  # Needs callback
        return 'callback'
    elif auto_qa_status == 'failed' or request_status == 'failed':  # Failed
        return 'failed'
    else:  # Everything else
        return 'processing'

def extract_date(ticket):  # Pull date from ticket
    """Extract date from various date fields"""
    date_str = None  # Start empty
    
    if 'ticket_assigned_date' in ticket and ticket['ticket_assigned_date']:  # Try assigned date first
        date_str = str(ticket['ticket_assigned_date']).split()[0]  # Get date part
    
    elif 'created_at' in ticket and ticket['created_at']:  # Try created date
        if isinstance(ticket['created_at'], dict) and '$date' in ticket['created_at']:  # MongoDB format
            date_str = ticket['created_at']['$date'].split('T')[0]  # Extract date
        else:  # String format
            date_str = str(ticket['created_at']).split('T')[0]  # Split at T
    
    elif 'updated_at' in ticket and ticket['updated_at']:  # Last resort
        if isinstance(ticket['updated_at'], dict) and '$date' in ticket['updated_at']:  # MongoDB format
            date_str = ticket['updated_at']['$date'].split('T')[0]  # Extract date
        else:  # String format
            date_str = str(ticket['updated_at']).split('T')[0]  # Split at T
    
    if date_str:  # Got a date
        try:  # Parse it
            return datetime.strptime(date_str, '%Y-%m-%d')  # Convert to datetime
        except ValueError:  # Bad format
            pass  # Use fallback
    
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Default to today

@app.route('/')  # Main page
def dashboard():
    return render_template('dashboard.html')  # Show dashboard

@app.route('/api/ticket-data')  # Get tickets API
def get_ticket_data():
    """API endpoint to fetch ticket data from MongoDB Atlas filtered by date"""
    if not client:  # No database
        return jsonify({'error': 'Database connection not available'}), 500  # Error
    
    selected_date = request.args.get('date')  # Get date from URL
    
    if selected_date:  # Date provided
        try:  # Parse it
            filter_date = datetime.strptime(selected_date, '%Y-%m-%d')  # Convert
        except ValueError:  # Bad format
            return jsonify({'error': 'Invalid date format'}), 400  # Error
    else:  # No date
        filter_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Use today
    
    all_tickets = list(collection.find({}, {'_id': 0}))  # Get all tickets
    
    filtered_tickets = []  # Start empty
    for ticket in all_tickets:  # Check each
        ticket_date = extract_date(ticket)  # Get date
        if ticket_date.date() == filter_date.date():  # Match?
            filtered_tickets.append(ticket)  # Add it
    
    if not filtered_tickets:  # Nothing found
        return jsonify({  # Empty response
            'client_ids': [],
            'completed': [],
            'processing': [],
            'failed': [],
            'callback': [],
            'message': 'No data available for this date'
        })
    
    client_stats = {}  # Store counts by client
    for ticket in filtered_tickets:  # Process each
        client_id = str(ticket.get('client_id', 'Unknown'))  # Get client ID
        status = parse_ticket_status(ticket)  # Get status
        
        if client_id not in client_stats:  # New client
            client_stats[client_id] = {  # Initialize
                'completed': 0,
                'processing': 0,
                'failed': 0,
                'callback': 0
            }
        
        client_stats[client_id][status] = client_stats[client_id].get(status, 0) + 1  # Count it
    
    client_ids = sorted(client_stats.keys())  # Sort clients
    completed_tickets = [client_stats[cid]['completed'] for cid in client_ids]  # Build arrays
    processing_tickets = [client_stats[cid]['processing'] for cid in client_ids]
    failed_tickets = [client_stats[cid]['failed'] for cid in client_ids]
    callback_tickets = [client_stats[cid]['callback'] for cid in client_ids]
    
    return jsonify({  # Send response
        'client_ids': client_ids,
        'completed': completed_tickets,
        'processing': processing_tickets,
        'failed': failed_tickets,
        'callback': callback_tickets
    })

@app.route('/api/available-dates')  # Get dates with data
def get_available_dates():
    """Get list of dates that have data"""
    if not client:  # No database
        return jsonify({'dates': []}), 500  # Empty
    
    all_tickets = list(collection.find({}, {'_id': 0}))  # Get all tickets
    
    dates_set = set()  # Unique dates only
    for ticket in all_tickets:  # Check each
        ticket_date = extract_date(ticket)  # Get date
        dates_set.add(ticket_date.strftime('%Y-%m-%d'))  # Add formatted
    
    date_list = sorted(list(dates_set), reverse=True)  # Newest first
    
    return jsonify({'dates': date_list})  # Send back

@app.route('/api/stats')  # Overall stats
def get_stats():
    """Get overall statistics"""
    if not client:  # No database
        return jsonify({'error': 'Database connection not available'}), 500  # Error
    
    all_tickets = list(collection.find({}, {'_id': 0}))  # Get everything
    
    stats = {  # Initialize counts
        'total_tickets': len(all_tickets),
        'completed': 0,
        'processing': 0,
        'failed': 0,
        'callback': 0
    }
    
    for ticket in all_tickets:  # Count each
        status = parse_ticket_status(ticket)  # Get status
        stats[status] = stats.get(status, 0) + 1  # Increment
    
    return jsonify(stats)  # Return

@app.route('/reload-data')  # Refresh data
def reload_data():
    """Reload data from MongoDB Atlas"""
    try:  # Try reload
        if not client:  # No connection
            return jsonify({'status': 'error', 'message': 'Database connection not available'}), 500  # Error
        
        client.admin.command('ping')  # Refresh connection
        count = collection.count_documents({})  # Count docs
        
        return jsonify({  # Success
            'status': 'success',
            'message': f'Data reloaded successfully. Found {count} documents.'
        })
    except Exception as e:  # Failed
        return jsonify({'status': 'error', 'message': str(e)}), 500  # Error

if __name__ == '__main__':  # Run directly
    if client:  # Connected?
        print("Starting Flask application...")  # Starting
        print(f"Connected to database: {db.name}")  # Show DB
        print(f"Using collection: {collection.name}")  # Show collection
        app.run(debug=True, host='0.0.0.0', port=5000)  # Start server
    else:  # No connection
        print("Cannot start application - MongoDB connection failed")  # Can't start
