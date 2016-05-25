#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
from datetime import datetime

import ijson
import argparse


def create_feature(obj, filter):
    """
        Creates the GeoJSON feature from the JSON object provided if not to be filtered out. Returns None if to be filtered out due to filter criteria.
    """

    activities_filter = filter.get("activities", None)
    confidence_threshold = filter.get("confidence_threshold", None)
    if activities_filter is not None:
        #print obj
        activities = set()
        if confidence_threshold is not None:
            #print str(obj['activities'])
            activities = set(activity for activity, confidence in obj['activities'].iteritems() if confidence >= confidence_threshold)
            #print str(activities)

        #print str(activities_filter)
        if not set(activities).intersection(activities_filter):
            #print "hat?"
            return None

    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [
                obj['longitudeE7'] / 10000000.0,
                obj['latitudeE7'] / 10000000.0
            ]
        },
        'properties': {
            'accuracy': obj.get('accuracy', None),
            'timestamp': datetime.fromtimestamp(int(obj['timestampMs']) / 1000.0).isoformat()
        }
    }


def parse_location(stream, filter):
    """
        Given a stream and a filter, parse JSON data that fits filter to GeoJSON file
    """

    parser = ijson.parse(stream)
    reading = False
    obj = {}
    key = None
    value = None
    for prefix, event, value in parser:
        #print "prefix: " + str(prefix)
        if prefix == 'locations' and event == 'start_array':
            reading = True
        elif prefix == 'locations' and event == 'end_array':
            reading = False
        elif reading:
            if event == 'start_map' and prefix == 'locations.item':
                obj = {}
                activities = {}
            elif event == 'end_map' and prefix == 'locations.item':
                obj['activities'] = activities
                yield create_feature(obj, filter)
            elif event == 'map_key':
                key = value
            elif prefix == 'locations.item.%s' % key and value is not None:
                obj[key] = value
            elif prefix == 'locations.item.activitys.item.activities.item.type':
                activity = value
            elif prefix == 'locations.item.activitys.item.activities.item.confidence':
                confidence = value
            elif prefix == 'locations.item.activitys.item.activities.item' and  event == 'end_map':
                activities[activity] = confidence


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Parse Google Location JSON data to GeoJSON")

    parser.add_argument('-i', '--input-file', default=None, dest='input_file', required=True)

    parser.add_argument('-a', '--activity', default=None, dest="activity_filter", help="Only include the activity(ies) given. Choose among: still, unknown, inVehicle, onBicycle, tilting, walking, onFoot, exitingVehicle, running. Use comma to separate multiple activities.")

    parser.add_argument('-t', '--confidence-threshold', default=0, type=int, dest="confidence_threshold", help="Only include coordinates and activities with equal or higher value than the threshold [0-100]")

    args = parser.parse_args()

    activities = None
    if args.activity_filter:
        activities = set(map(str.strip, args.activity_filter.split(",")))
        print activities

    with open(args.input_file, 'r') as file:
        for feature in parse_location(file, filter={"activities": activities, "confidence_threshold": args.confidence_threshold}):
            if feature is not None:
                print feature
