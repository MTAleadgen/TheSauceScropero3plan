#!/usr/bin/env python3
"""
lambda_qwen_handler.py

AWS Lambda handler for extracting structured information from event descriptions
using Alibaba's Qwen 3 large language model.
"""

import os
import json
import logging
import boto3
import dashscope
from dashscope import Generation
from dashscope.api_entities.dashscope_response import GenerationOutput

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configure DashScope API key (Qwen access)
DASHSCOPE_API_KEY = os.environ.get('DASHSCOPE_API_KEY')
dashscope.api_key = DASHSCOPE_API_KEY

def extract_fields_from_description(description, title, fields_to_extract):
    """
    Use Qwen 3 to extract specified fields from event description text.
    
    Args:
        description: Event description text
        title: Event title 
        fields_to_extract: List of fields to extract
        
    Returns:
        Dict containing extracted field values
    """
    if not description:
        logger.warning("Empty description provided")
        return {}
    
    # Build prompt for Qwen
    field_list = ", ".join(fields_to_extract)
    
    prompt = f"""
Extract structured information from the following event description.
Event title: {title}
Event description: {description}

Please extract the following information: {field_list}

For price, extract any price mentioned (including free, donation, ticket price ranges).
For eventAttendanceMode, determine if the event is online, in-person, or hybrid.
For eventStatus, indicate if the event is scheduled, postponed, or canceled.
For organizer_name, identify who is organizing or hosting the event.
For additional_location_details, extract any venue address or location information.

Format your response as a JSON object with these fields. If you can't find information for a field, leave it as null.
"""

    try:
        response = Generation.call(
            model='qwen3-max',
            prompt=prompt,
            result_format='json',
            temperature=0.1,  # Low temperature for more factual/consistent responses
            max_tokens=1000,
        )
        
        # Handle response
        if response.status_code == 200:
            # Parse the model's JSON response
            try:
                # Qwen should return a JSON object directly due to result_format='json'
                if hasattr(response, 'output') and hasattr(response.output, 'choices'):
                    result = response.output.choices[0].message.content
                    # If result is already a dict, use it directly
                    if isinstance(result, dict):
                        return result
                    # Otherwise assume it's a JSON string
                    return json.loads(result)
                else:
                    logger.warning(f"Unexpected response structure: {response}")
                    return {}
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from LLM response: {e}")
                return {}
        else:
            logger.error(f"API call failed with status {response.status_code}: {response.message}")
            return {}
            
    except Exception as e:
        logger.error(f"Error calling Qwen API: {e}")
        return {}

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Args:
        event: Lambda event object containing:
            - event_id: ID of the event
            - description: Event description text
            - title: Event title
            - fields_to_extract: List of fields to extract
        context: Lambda context
        
    Returns:
        Dict containing the extracted information
    """
    try:
        # Log the event
        logger.info(f"Received event: {event}")
        
        # Extract inputs
        event_id = event.get('event_id')
        description = event.get('description', '')
        title = event.get('title', '')
        fields_to_extract = event.get('fields_to_extract', [])
        
        if not description:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No description provided'})
            }
        
        if not fields_to_extract:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No fields to extract specified'})
            }
            
        # Call Qwen to extract information
        extracted_data = extract_fields_from_description(description, title, fields_to_extract)
        
        # Return the results
        return {
            'statusCode': 200,
            'event_id': event_id,
            'extracted_data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# For local testing
if __name__ == "__main__":
    # Example event for testing
    test_event = {
        'event_id': 'test-123',
        'description': """
        Join us for an unforgettable evening at the Grand Ballroom on Saturday, June 15th at 7 PM! 
        Tickets are $25 in advance or $30 at the door. This hybrid event will feature live music 
        by the Big Band Orchestra and will be streamed online for those who can't attend in person. 
        All proceeds benefit the City Arts Foundation. Hosted by the Downtown Cultural Committee.
        The Grand Ballroom is located at 123 Main Street, Downtown District.
        """,
        'title': 'Summer Charity Concert',
        'fields_to_extract': [
            'price', 
            'eventAttendanceMode', 
            'eventStatus', 
            'organizer_name',
            'additional_location_details'
        ]
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2)) 