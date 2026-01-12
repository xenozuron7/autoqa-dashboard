from dotenv import load_dotenv  # load environment variables from .env file
load_dotenv()  # initialize environment variables

from flask import Flask, render_template, jsonify, request  # import flask components
from pymongo import MongoClient  # import mongodb client
from datetime import datetime, timedelta  # import date/time utilities
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
    """Extract date from various date fields"""
    date_obj = None  # initialize date object
    
    if 'created_at' in ticket and ticket['created_at']:  # try created_at first
        date_obj = ticket['created_at']
    elif 'updated_at' in ticket and ticket['updated_at']:  # try updated_at as fallback
        date_obj = ticket['updated_at']
    
    if not date_obj:  # if no date found
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # return today
    
    if isinstance(date_obj, datetime):  # handle datetime objects
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)  # return normalized date
    
    if isinstance(date_obj, str):  # handle string formats
        try:  # attempt to parse string
            date_str = date_obj.split('T')[0]  # remove time portion
            return datetime.strptime(date_str, '%Y-%m-%d')  # convert to datetime
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
    
    query = {  # build mongodb query filter
        'created_at': {  # filter on created_at field
            '$gte': start_of_day,  # greater than or equal to start
            '$lt': end_of_day  # less than end
        }
    }
    
    filtered_tickets = list(collection.find(query, {'_id': 0}))  # retrieve filtered tickets
    
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
        client_id = str(ticket.get('client_id', 'Unknown'))  # extract client id
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
    
    query = {  # build mongodb query filter
        'created_at': {  # filter on created_at field
            '$gte': start_of_day,  # greater than or equal to start
            '$lt': end_of_day  # less than end
        }
    }
    
    all_tickets = list(collection.find(query, {'_id': 0}).sort('created_at', -1))  # retrieve all tickets for date
    
    # filter tickets by status
    filtered_tickets = []  # initialize filtered list
    for ticket in all_tickets:  # process each ticket
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
    
    return jsonify({  # return formatted response
        'date': selected_date,
        'status': status_filter,
        'total_tickets': len(filtered_tickets),
        'tickets': filtered_tickets
    })


@app.route('/api/available-dates')  # define available dates api endpoint
def get_available_dates():  # retrieve list of dates with available data
    """Get list of dates that have data using aggregation"""
    if not client:  # verify database connection
        return jsonify({'dates': []}), 500  # return empty response
    
    pipeline = [  # build aggregation pipeline
        {'$project': {  # extract just the date part
            'date_only': {  # create new field with formatted date
                '$dateToString': {  # convert datetime to string
                    'format': '%Y-%m-%d',  # specify date format
                    'date': '$created_at'  # use created_at field
                }
            }
        }},
        {'$group': {'_id': '$date_only'}},  # group by unique dates
        {'$sort': {'_id': -1}}  # sort descending (newest first)
    ]
    
    results = collection.aggregate(pipeline)  # execute aggregation
    date_list = [doc['_id'] for doc in results if doc['_id']]  # extract dates
    
    return jsonify({'dates': date_list})  # return dates as json


@app.route('/api/client-tickets')  # define client tickets api endpoint
def get_client_tickets():  # retrieve tickets for specific client
    """API endpoint to fetch all tickets for a specific client"""
    
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    client_id = request.args.get('client_id')  # get client_id parameter from query string
    
    if not client_id:  # no client_id provided
        return jsonify({'error': 'Client ID is required'}), 400  # return validation error
    
    query = {'client_id': client_id}  # build query filter
    tickets = list(collection.find(query, {'_id': 0}).sort('created_at', -1))  # get tickets sorted by date (newest first)
    
    if not tickets:  # no tickets found for this client
        return jsonify({  # return empty response
            'client_id': client_id,
            'total_tickets': 0,
            'tickets': [],
            'stats': {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0},
            'message': 'No tickets found for this client'
        })
    
    stats = {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}  # initialize stats
    
    for ticket in tickets:  # process each ticket
        status = parse_ticket_status(ticket)  # determine ticket status
        stats[status] = stats.get(status, 0) + 1  # increment status counter
    
    formatted_tickets = []  # initialize formatted list
    for ticket in tickets:  # process each ticket
        formatted_tickets.append({  # add formatted ticket data
            'ticket_id': ticket.get('ticket_id', 'N/A'),
            'status': parse_ticket_status(ticket),
            'created_at': extract_date(ticket).strftime('%Y-%m-%d %H:%M'),
            'auto_qa_status': ticket.get('auto_qa_status', 'N/A'),
            'request_status': ticket.get('request_status', 'N/A')
        })
    
    return jsonify({  # return formatted response
        'client_id': client_id,
        'total_tickets': len(tickets),
        'tickets': formatted_tickets,
        'stats': stats
    })


@app.route('/api/all-clients')  # define all clients api endpoint
def get_all_clients():  # retrieve list of all unique client ids
    """API endpoint to get list of all client IDs"""
    
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    pipeline = [  # build aggregation pipeline
        {'$group': {'_id': '$client_id'}},  # group by client_id
        {'$sort': {'_id': 1}}  # sort alphabetically
    ]
    
    results = collection.aggregate(pipeline)  # execute aggregation
    client_ids = [doc['_id'] for doc in results if doc['_id']]  # extract client ids
    
    return jsonify({'client_ids': client_ids})  # return client ids as json


@app.route('/api/stats')  # define statistics api endpoint
def get_stats():  # retrieve overall ticket statistics
    """Get overall statistics"""
    if not client:  # verify database connection
        return jsonify({'error': 'Database connection not available'}), 500  # return connection error
    
    all_tickets = list(collection.find({}, {'_id': 0}))  # retrieve all tickets from collection
    
    stats = {  # initialize statistics dictionary
        'total_tickets': len(all_tickets),
        'completed': 0,
        'processing': 0,
        'failed': 0,
        'callback': 0
    }
    
    for ticket in all_tickets:  # iterate through all tickets
        status = parse_ticket_status(ticket)  # determine ticket status
        stats[status] = stats.get(status, 0) + 1  # increment status counter
    
    return jsonify(stats)  # return statistics as json


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
