from dotenv import load_dotenv  # load environment variables from .env file
load_dotenv()  # initialize environment variables

from flask import Flask, render_template, jsonify, request  # import flask components
from pymongo import MongoClient  # import mongodb client
from datetime import datetime, timedelta  # import date/time utilities
from dateutil import parser  # import date parser for flexible parsing
import os  # import operating system utilities

app = Flask(__name__)  # initialize flask application
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-fallback')  # configure secret key

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')  # retrieve mongodb connection string

try:  # attempt database connection
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)  # connect with 5 second timeout
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
    
    # Try created_at first
    if 'created_at' in ticket and ticket['created_at']:  # try created_at first
        date_obj = ticket['created_at']
    elif 'updated_at' in ticket and ticket['updated_at']:  # try updated_at as fallback
        date_obj = ticket['updated_at']
    
    if not date_obj:  # if no date found
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # return today
    
    # Handle MongoDB $date format
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

@app.route('/')  # define root route
def dashboard():  # serve dashboard page
    return render_template('dashboard.html')  # render dashboard template

@app.route('/api/ticket-data')  # define ticket data api endpoint
def get_ticket_data():  # retrieve and filter ticket data by date
    """API endpoint to fetch ticket data from MongoDB filtered by date"""
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    selected_date = request.args.get('date')  # get date parameter from query string
    
    try:  # attempt to parse date
        filter_date = datetime.strptime(selected_date, '%Y-%m-%d') if selected_date else datetime.now()  # parse date or use current
    except ValueError:  # handle invalid date format
        return jsonify({'error': 'Invalid date format'}), 400  # return validation error
    
    start_of_day = filter_date.replace(hour=0, minute=0, second=0, microsecond=0)  # calculate start of day
    end_of_day = start_of_day + timedelta(days=1)  # calculate end of day
    
    # Fetch all tickets and filter in Python due to MongoDB date format
    all_tickets = list(collection.find({}, {'_id': 0}))  # retrieve all tickets
    
    filtered_tickets = []  # initialize filtered list
    for ticket in all_tickets:  # loop through tickets
        ticket_date = extract_date(ticket)  # extract date
        if start_of_day <= ticket_date < end_of_day:  # check if in date range
            filtered_tickets.append(ticket)  # add to filtered list
    
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
    
    # Fetch all tickets and filter in Python
    all_tickets = list(collection.find({}, {'_id': 0}))  # retrieve all tickets
    
    filtered_tickets = []  # initialize filtered list
    for ticket in all_tickets:  # process each ticket
        ticket_date = extract_date(ticket)  # extract date
        if start_of_day <= ticket_date < end_of_day:  # check date range
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
        'tickets': filtered_tickets
    })

@app.route('/api/client-tickets-7days')  # define client data api endpoint
def get_client_tickets_7days():  # get client ticket data - ALL TIME for cards, 7 DAYS for graphs
    """Get client ticket data - total count for stat cards, past 7 days for graph visualization"""
    if not client:  # verify database connection
        return jsonify({'success': False, 'error': 'Database connection not available'}), 500  # return connection error
    
    client_id = request.args.get('client_id')  # get client id parameter
    
    if not client_id:  # if no client id provided
        return jsonify({'success': False, 'error': 'Client ID required'}), 400  # return error
    
    try:  # attempt to fetch data
        # Fetch ALL tickets for this client
        all_client_tickets = list(collection.find({'client_id': str(client_id)}, {'_id': 0}))  # query by string client_id
        
        if not all_client_tickets:  # if no tickets found
            print(f"No tickets found for client {client_id}")  # debug log
            return jsonify({  # return empty data structure
                'success': True,
                'total_tickets': 0,
                'all_time_stats': {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0},
                'seven_day_data': {
                    'dates': get_last_n_days_labels_from_date(datetime.now(), 7),  # get 7 day labels
                    'completed': [0] * 7,  # zero array for 7 days
                    'processing': [0] * 7,  # zero array for 7 days
                    'failed': [0] * 7,  # zero array for 7 days
                    'callback': [0] * 7  # zero array for 7 days
                }
            })
        
        # Get the date range from actual tickets
        ticket_dates = [extract_date(t) for t in all_client_tickets]  # extract all dates
        earliest_date = min(ticket_dates)  # find earliest
        latest_date = max(ticket_dates)  # find latest
        
        print(f"\n=== Client {client_id} ===")  # debug log
        print(f"Total tickets (all time): {len(all_client_tickets)}")  # debug log
        print(f"Date range in data: {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")  # debug log
        
        # Calculate ALL TIME stats for the stat cards
        all_time_stats = {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}  # initialize all time counters
        
        for ticket in all_client_tickets:  # loop through ALL tickets
            status = parse_ticket_status(ticket)  # get ticket status
            if status in all_time_stats:  # if status is valid
                all_time_stats[status] += 1  # increment counter
        
        print(f"All time stats: {all_time_stats}")  # debug log
        
        # Now calculate 7-day data for graphs ONLY
        # Use the latest ticket date as the end date for the range
        end_date = latest_date.replace(hour=23, minute=59, second=59, microsecond=999)  # use latest ticket date
        start_date = (end_date - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)  # 7 days before latest
        
        print(f"Graph range (7 days): {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")  # debug log
        
        # Filter tickets by 7-day date range for graphs
        seven_day_tickets = []  # initialize filtered tickets list for graphs
        for ticket in all_client_tickets:  # loop through all client tickets
            ticket_date = extract_date(ticket)  # get ticket date
            if start_date <= ticket_date <= end_date:  # check if within 7-day range
                seven_day_tickets.append(ticket)  # add to filtered list
        
        print(f"Tickets in 7-day range (for graphs): {len(seven_day_tickets)}")  # debug log
        
        # Organize 7-day tickets for graph visualization
        seven_day_breakdown = organize_tickets_by_date_from_end(seven_day_tickets, end_date)  # organize tickets by date from end date
        
        print(f"Returning data: {len(all_client_tickets)} total tickets, {all_time_stats}")  # debug log
        
        return jsonify({  # return success response
            'success': True,
            'total_tickets': len(all_client_tickets),  # total ticket count (all time)
            'all_time_stats': all_time_stats,  # all time status totals for stat cards
            'seven_day_data': seven_day_breakdown  # 7-day daily breakdown for graphs
        })
        
    except Exception as e:  # handle errors
        print(f'Error getting client data: {e}')  # log error
        import traceback  # import traceback module
        traceback.print_exc()  # print full error trace
        return jsonify({'success': False, 'error': str(e)}), 500  # return error response

def get_last_n_days_labels_from_date(end_date, n):  # generate labels for last n days from specific date
    """Generate labels for last n days with full date format"""
    labels = []  # initialize labels list
    
    for i in range(n-1, -1, -1):  # iterate backwards n days
        date = end_date - timedelta(days=i)  # calculate date from end
        label = date.strftime('%b %d, %Y')  # format as "Jan 05, 2026"
        labels.append(label)  # add label
    
    return labels  # return labels list

def organize_tickets_by_date_from_end(tickets, end_date):  # group tickets by date from end date
    """Group tickets by date and status for 7-day view from specific end date"""
    seven_days = {}  # initialize date dictionary
    
    for i in range(6, -1, -1):  # iterate backwards 7 days
        date = end_date - timedelta(days=i)  # calculate date from end
        date_str = date.strftime('%Y-%m-%d')  # format date as string for key
        label = date.strftime('%b %d, %Y')  # format as "Jan 05, 2026" for display
        
        seven_days[date_str] = {  # initialize date entry
            'label': label,  # store label
            'completed': 0,  # initialize completed count
            'processing': 0,  # initialize processing count
            'failed': 0,  # initialize failed count
            'callback': 0  # initialize callback count
        }
    
    for ticket in tickets:  # loop through tickets
        ticket_date = extract_date(ticket)  # get ticket date
        date_str = ticket_date.strftime('%Y-%m-%d')  # format date string
        
        if date_str in seven_days:  # if date is in range
            status = parse_ticket_status(ticket)  # get ticket status
            if status in seven_days[date_str]:  # if status is valid
                seven_days[date_str][status] += 1  # increment count
    
    dates = []  # initialize dates list
    completed_data = []  # initialize completed data list
    processing_data = []  # initialize processing data list
    failed_data = []  # initialize failed data list
    callback_data = []  # initialize callback data list
    
    for i in range(6, -1, -1):  # iterate backwards 7 days
        date = end_date - timedelta(days=i)  # calculate date from end
        date_str = date.strftime('%Y-%m-%d')  # format date string
        
        if date_str in seven_days:  # if date exists in dictionary
            dates.append(seven_days[date_str]['label'])  # add label to list
            completed_data.append(seven_days[date_str]['completed'])  # add completed count
            processing_data.append(seven_days[date_str]['processing'])  # add processing count
            failed_data.append(seven_days[date_str]['failed'])  # add failed count
            callback_data.append(seven_days[date_str]['callback'])  # add callback count
    
    return {  # return organized data
        'dates': dates,
        'completed': completed_data,
        'processing': processing_data,
        'failed': failed_data,
        'callback': callback_data
    }

@app.route('/api/all-clients')  # define all clients api endpoint
def get_all_clients():  # retrieve list of all unique client ids
    """API endpoint to get list of all client IDs"""
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    try:  # attempt to fetch clients
        client_ids = collection.distinct('client_id')  # get distinct client ids
        client_ids = sorted([str(cid) for cid in client_ids if cid is not None])  # convert to strings and sort
        
        print(f"Found {len(client_ids)} unique clients")  # debug log
        
        return jsonify({'client_ids': client_ids})  # return client ids as json
    except Exception as e:  # handle errors
        print(f"Error getting clients: {e}")  # log error
        return jsonify({'error': str(e), 'client_ids': []}), 500  # return error

@app.route('/reload-data')  # define data reload endpoint
def reload_data():  # reload data from mongodb
    """Reload data from MongoDB Atlas"""
    try:  # attempt to reload data
        if not client:  # verify database connection
            return jsonify({'status': 'error', 'message': 'Database connection not available'}), 500  # return connection error
        
        client.admin.command('ping')  # refresh connection with ping
        count = collection.count_documents({})  # count total documents
        
        return jsonify({  # return success response
            'status': 'success',
            'message': f'Data reloaded successfully. Found {count} documents.'
        })
    except Exception as e:  # handle reload errors
        return jsonify({'status': 'error', 'message': str(e)}), 500  # return error response

if __name__ == '__main__':  # check if running as main script
    if client:  # verify database connection exists
        print("Starting Flask application...")  # log application start
        print(f"Connected to database: {db.name}")  # log database name
        print(f"Using collection: {collection.name}")  # log collection name
        app.run(debug=True, host='0.0.0.0', port=5000)  # start flask development server
    else:  # no database connection
        print("Cannot start application - MongoDB connection failed")  # log connection failure
