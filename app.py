from dotenv import load_dotenv  # load environment variables from .env file
load_dotenv()  # initialize environment variables

from flask import Flask, render_template, jsonify, request  # import flask components
from pymongo import MongoClient, ASCENDING, DESCENDING  # import mongodb client and sort constants
from datetime import datetime, timedelta  # import date/time utilities
from dateutil import parser  # import date parser for flexible parsing
import os  # import operating system utilities

app = Flask(__name__)  # initialize flask application
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-fallback')  # configure secret key

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')  # retrieve mongodb connection string

try:  # attempt database connection
    # Connection pooling optimized for high volume
    client = MongoClient(
        MONGODB_URI,
        maxPoolSize=100,  # max 100 connections for high concurrency
        minPoolSize=20,  # min 20 connections always ready
        maxIdleTimeMS=45000,  # keep idle connections for 45 seconds
        serverSelectionTimeoutMS=5000,  # 5 second timeout
        retryWrites=True,  # retry failed writes
        retryReads=True,  # retry failed reads
        compressors='snappy,zlib'  # enable compression for network efficiency
    )
    client.admin.command('ping')  # verify connection
    print("✓ Successfully connected to MongoDB!")  # log success
    db = client['AutoQA']  # access database
    collection = db['auto_qa_results_demo']  # access collection
except Exception as e:  # handle connection failure
    print(f"✗ Failed to connect to MongoDB: {e}")  # log error
    client = None  # set client to none

def parse_ticket_status(ticket):  # parse ticket status from ticket data
    """Determine ticket status from auto_qa_status"""
    auto_qa_status = str(ticket.get('auto_qa_status', '')).lower()  # get status field
    request_status = str(ticket.get('request_status', '')).lower()  # backup status
    
    if auto_qa_status == 'complete':  # check if completed
        return 'completed'
    elif auto_qa_status == 'callback':  # check if callback needed
        return 'callback'
    elif auto_qa_status == 'failed' or request_status == 'failed':  # check if failed
        return 'failed'
    else:  # all other cases
        return 'processing'

def extract_date(ticket):  # extract date from ticket document
    """Extract date from various date fields including MongoDB $date format"""
    date_obj = None  # initialize date object
    
    if 'created_at' in ticket and ticket['created_at']:  # try created_at first
        date_obj = ticket['created_at']
    elif 'updated_at' in ticket and ticket['updated_at']:  # try updated_at as fallback
        date_obj = ticket['updated_at']
    
    if not date_obj:  # if no date found
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # return today
    
    if isinstance(date_obj, dict) and '$date' in date_obj:  # mongodb date format
        try:  # attempt to parse mongodb date
            date_str = date_obj['$date']  # extract date string
            parsed_date = parser.parse(date_str)  # parse iso format
            return parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)  # return normalized date
        except:  # parsing failed
            pass  # continue to other methods
    
    if isinstance(date_obj, datetime):  # handle datetime objects
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)  # return normalized date
    
    if isinstance(date_obj, str):  # handle string formats
        try:  # attempt to parse string
            parsed_date = parser.parse(date_obj)  # parse flexible format
            return parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)  # return normalized date
        except:  # handle parsing errors
            pass  # continue to fallback
    
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # default fallback

def build_date_query(start_date, end_date):  # build mongodb query for date filtering
    """Build MongoDB query for date range that handles multiple date formats"""
    date_query = {
        '$or': [
            {
                'created_at': {
                    '$gte': start_date,
                    '$lt': end_date
                }
            },
            {
                'created_at.$date': {
                    '$gte': start_date.isoformat(),
                    '$lt': end_date.isoformat()
                }
            }
        ]
    }
    return date_query

@app.route('/')  # define root route
def dashboard():  # serve dashboard page
    return render_template('dashboard.html')  # render dashboard template

@app.route('/api/ticket-data')  # define ticket data api endpoint
def get_ticket_data():  # retrieve and filter ticket data by date
    """API endpoint to fetch ticket data from MongoDB filtered by date - optimized for millions"""
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    selected_date = request.args.get('date')  # get date parameter from query string
    
    try:  # attempt to parse date
        filter_date = datetime.strptime(selected_date, '%Y-%m-%d') if selected_date else datetime.now()  # parse date or use current
    except ValueError:  # handle invalid date format
        return jsonify({'error': 'Invalid date format'}), 400  # return validation error
    
    start_of_day = filter_date.replace(hour=0, minute=0, second=0, microsecond=0)  # calculate start of day
    end_of_day = start_of_day + timedelta(days=1)  # calculate end of day
    
    # Build date query for MongoDB
    date_query = build_date_query(start_of_day, end_of_day)
    
    # Use find with projection for better performance - let MongoDB choose best index
    projection = {
        '_id': 0,
        'client_id': 1,
        'auto_qa_status': 1,
        'request_status': 1
    }
    
    # Fetch tickets - MongoDB will automatically use best available index
    filtered_tickets = list(collection.find(date_query, projection).limit(100000))
    
    if not filtered_tickets:  # check if no tickets found
        return jsonify({  # return empty response
            'client_ids': [],
            'completed': [],
            'processing': [],
            'failed': [],
            'callback': [],
            'message': 'No data available for this date'
        })
    
    client_stats = {}  # initialize client statistics dictionary
    
    for ticket in filtered_tickets:  # process each filtered ticket
        client_id = str(ticket.get('client_id', 'Unknown'))  # extract client id as string
        status = parse_ticket_status(ticket)  # determine ticket status
        
        if client_id not in client_stats:  # check if client entry exists
            client_stats[client_id] = {  # initialize client statistics
                'completed': 0,
                'processing': 0,
                'failed': 0,
                'callback': 0
            }
        
        client_stats[client_id][status] = client_stats[client_id].get(status, 0) + 1  # increment status counter
    
    client_ids = sorted(client_stats.keys())  # sort client ids alphabetically
    
    return jsonify({  # return formatted json response
        'client_ids': client_ids,
        'completed': [client_stats[c]['completed'] for c in client_ids],
        'processing': [client_stats[c]['processing'] for c in client_ids],
        'failed': [client_stats[c]['failed'] for c in client_ids],
        'callback': [client_stats[c]['callback'] for c in client_ids]
    })

@app.route('/api/detailed-tickets')  # define detailed tickets api endpoint
def get_detailed_tickets():  # retrieve detailed ticket list by date and status
    """API endpoint to fetch detailed ticket list for a specific date and status"""
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    selected_date = request.args.get('date')  # get date parameter
    status_filter = request.args.get('status')  # get status parameter
    
    if not selected_date or not status_filter:  # missing parameters
        return jsonify({'error': 'Date and status are required'}), 400  # return validation error
    
    try:  # attempt to parse date
        filter_date = datetime.strptime(selected_date, '%Y-%m-%d')  # parse date string
    except ValueError:  # handle invalid date format
        return jsonify({'error': 'Invalid date format'}), 400  # return validation error
    
    start_of_day = filter_date.replace(hour=0, minute=0, second=0, microsecond=0)  # calculate start of day
    end_of_day = start_of_day + timedelta(days=1)  # calculate end of day
    
    # Build date query for MongoDB
    date_query = build_date_query(start_of_day, end_of_day)
    
    # Optimized query with projection
    projection = {
        '_id': 0,
        'ticket_id': 1,
        'client_id': 1,
        'auto_qa_status': 1,
        'request_status': 1,
        'created_at': 1,
        'updated_at': 1
    }
    
    # Fetch with limit
    date_filtered_tickets = list(collection.find(date_query, projection).limit(10000))
    
    filtered_tickets = []  # initialize filtered list
    for ticket in date_filtered_tickets:  # process each ticket
        ticket_status = parse_ticket_status(ticket)  # determine status
        if ticket_status == status_filter:  # matches requested status
            filtered_tickets.append({  # add to list
                'ticket_id': ticket.get('ticket_id', 'N/A'),
                'client_id': ticket.get('client_id', 'N/A'),
                'status': ticket_status,
                'created_at': extract_date(ticket).strftime('%Y-%m-%d %H:%M'),
                'auto_qa_status': ticket.get('auto_qa_status', 'N/A'),
                'request_status': ticket.get('request_status', 'N/A')
            })
    
    # Sort by created_at descending
    filtered_tickets.sort(key=lambda x: x['created_at'], reverse=True)  # sort by date
    
    return jsonify({  # return formatted response
        'date': selected_date,
        'status': status_filter,
        'total_tickets': len(filtered_tickets),
        'tickets': filtered_tickets[:1000]  # limit to 1000 for UI
    })

@app.route('/api/client-tickets-7days')  # define client data api endpoint
def get_client_tickets_7days():  # get client ticket data
    """Get client ticket data - past 7 days optimized for millions"""
    if not client:  # verify database connection
        return jsonify({'success': False, 'error': 'Database connection not available'}), 500  # return connection error
    
    client_id = request.args.get('client_id')  # get client id parameter
    
    if not client_id:  # if no client id provided
        return jsonify({'success': False, 'error': 'Client ID required'}), 400  # return error
    
    try:  # attempt to fetch data
        end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999)  # current date/time
        start_date = (end_date - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)  # 7 days before today
        
        print(f"\n=== Client {client_id} ===")  # debug log
        print(f"7-day range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")  # debug log
        
        # Build query
        mongo_query = {
            'client_id': str(client_id),
            '$or': [
                {
                    'created_at': {
                        '$gte': start_date,
                        '$lte': end_date
                    }
                },
                {
                    'created_at.$date': {
                        '$gte': start_date.isoformat(),
                        '$lte': end_date.isoformat()
                    }
                }
            ]
        }
        
        # Projection
        projection = {
            '_id': 0,
            'auto_qa_status': 1,
            'request_status': 1,
            'created_at': 1,
            'updated_at': 1
        }
        
        # Fetch tickets
        seven_day_tickets = list(collection.find(mongo_query, projection))
        
        print(f"Tickets found: {len(seven_day_tickets)}")  # debug log
        
        if not seven_day_tickets:  # if no tickets found
            return jsonify({  # return empty data
                'success': True,
                'total_tickets': 0,
                'all_time_stats': {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0},
                'seven_day_data': {
                    'dates': get_last_n_days_labels_from_date(end_date, 7),
                    'completed': [0] * 7,
                    'processing': [0] * 7,
                    'failed': [0] * 7,
                    'callback': [0] * 7
                }
            })
        
        # Calculate stats
        seven_day_stats = {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}
        for ticket in seven_day_tickets:
            status = parse_ticket_status(ticket)
            if status in seven_day_stats:
                seven_day_stats[status] += 1
        
        # Organize by date
        seven_day_breakdown = organize_tickets_by_date_from_end(seven_day_tickets, end_date)
        
        return jsonify({
            'success': True,
            'total_tickets': len(seven_day_tickets),
            'all_time_stats': seven_day_stats,
            'seven_day_data': seven_day_breakdown
        })
        
    except Exception as e:  # handle errors
        print(f'Error getting client data: {e}')  # log error
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

def get_last_n_days_labels_from_date(end_date, n):  # generate date labels
    """Generate labels for last n days"""
    labels = []
    for i in range(n-1, -1, -1):
        date = end_date - timedelta(days=i)
        label = date.strftime('%b %d, %Y')
        labels.append(label)
    return labels

def organize_tickets_by_date_from_end(tickets, end_date):  # organize tickets by date
    """Group tickets by date and status"""
    seven_days = {}
    
    for i in range(6, -1, -1):
        date = end_date - timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        label = date.strftime('%b %d, %Y')
        seven_days[date_str] = {
            'label': label,
            'completed': 0,
            'processing': 0,
            'failed': 0,
            'callback': 0
        }
    
    for ticket in tickets:
        ticket_date = extract_date(ticket)
        date_str = ticket_date.strftime('%Y-%m-%d')
        if date_str in seven_days:
            status = parse_ticket_status(ticket)
            if status in seven_days[date_str]:
                seven_days[date_str][status] += 1
    
    dates = []
    completed_data = []
    processing_data = []
    failed_data = []
    callback_data = []
    
    for i in range(6, -1, -1):
        date = end_date - timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        if date_str in seven_days:
            dates.append(seven_days[date_str]['label'])
            completed_data.append(seven_days[date_str]['completed'])
            processing_data.append(seven_days[date_str]['processing'])
            failed_data.append(seven_days[date_str]['failed'])
            callback_data.append(seven_days[date_str]['callback'])
    
    return {
        'dates': dates,
        'completed': completed_data,
        'processing': processing_data,
        'failed': failed_data,
        'callback': callback_data
    }

@app.route('/api/all-clients')  # define all clients endpoint
def get_all_clients():  # retrieve all client ids
    """Get list of all client IDs"""
    if not client:
        return jsonify({'error': 'Database connection not available'}), 500
    
    try:
        client_ids = collection.distinct('client_id')
        client_ids = sorted([str(cid) for cid in client_ids if cid is not None])
        print(f"Found {len(client_ids)} unique clients")
        return jsonify({'client_ids': client_ids})
    except Exception as e:
        print(f"Error getting clients: {e}")
        return jsonify({'error': str(e), 'client_ids': []}), 500

@app.route('/reload-data')  # define reload endpoint
def reload_data():  # reload data
    """Reload data from MongoDB"""
    try:
        if not client:
            return jsonify({'status': 'error', 'message': 'Database connection not available'}), 500
        
        client.admin.command('ping')
        count = collection.estimated_document_count()
        
        return jsonify({
            'status': 'success',
            'message': f'Data reloaded. Estimated {count:,} documents.'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def create_optimized_indexes():  # create indexes
    """Create optimized indexes for performance"""
    try:
        print("\n🔧 Creating indexes...")
        
        collection.create_index([("client_id", ASCENDING)], name="idx_client_id", background=True)
        print("   ✓ client_id")
        
        collection.create_index([("created_at", ASCENDING)], name="idx_created_at", background=True)
        print("   ✓ created_at")
        
        collection.create_index([("auto_qa_status", ASCENDING)], name="idx_auto_qa_status", background=True)
        print("   ✓ auto_qa_status")
        
        collection.create_index([("client_id", ASCENDING), ("created_at", DESCENDING)], name="idx_client_date", background=True)
        print("   ✓ compound (client_id + created_at)")
        
        collection.create_index([("created_at", ASCENDING), ("client_id", ASCENDING)], name="idx_date_client", background=True)
        print("   ✓ compound (created_at + client_id)")
        
        collection.create_index([("client_id", ASCENDING), ("auto_qa_status", ASCENDING), ("created_at", DESCENDING)], name="idx_client_status_date", background=True)
        print("   ✓ triple (client + status + date)")
        
        print("\n✅ All indexes created!")
        
    except Exception as e:
        print(f"⚠️  Index creation: {e} (may already exist)")

if __name__ == '__main__':
    if client:
        print("\nStarting Flask (optimized for 10M+ records)...")
        print(f"Database: {db.name}")
        print(f"Collection: {collection.name}")
        
        create_optimized_indexes()
        
        print("\n🚀 Ready!")
        print("   - Pool: 100 max, 20 min")
        print("   - Compression: Enabled")
        print("   - Auto-indexing: MongoDB optimizes queries\n")
        
        app.run(debug=True, host='0.0.0.0', port=5000)
    else:
        print("MongoDB connection failed")
