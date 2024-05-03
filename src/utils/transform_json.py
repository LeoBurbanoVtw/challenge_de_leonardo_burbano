import json

def transform_json(input_json_file: str, output_json_file) -> None:
    """
    Process tweets from an input JSON file and write processed data to a new JSONL file.

    Args:
        input_file (str): Path to the input JSON file containing tweets.
        output_file (str): Path to the output JSONL file to write processed data.

    Raises:
        IOError: If there is an issue reading or writing the file.
        ValueError: If there's a JSON decoding error or missing expected keys.

    """
    data = []
    try:
        with open(input_json_file, 'r') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    
                    # Extract tweet attributes
                    id = obj.get("id", "")
                    username = obj["user"]["username"]
                    userid = obj["user"]["id"]
                    date = obj.get("date", "")
                    content = obj.get("content", "")
                    
                    # Extract mentioned users
                    list_mentioned_users = []
                    mentioned_users = obj.get("mentionedUsers", [])
                    if mentioned_users:
                        for user in mentioned_users:
                            mentioned_user = {
                                "username": user.get("username", ""),
                                "userid": user.get("id", "")
                            }
                            list_mentioned_users.append(mentioned_user)

                    # Append processed tweet data to list
                    data.append({
                        "id": id,
                        "username": username,
                        "userid": userid,
                        "date": date,
                        "content": content,
                        "mentionedUsers": list_mentioned_users
                    })

                except (json.JSONDecodeError, KeyError) as e:
                    raise ValueError(f"Error processing tweet: {e}")

        # Writing the processed data to a new JSONL file
        with open(output_json_file, 'w') as outfile:
            for tweet in data:
                json.dump(tweet, outfile)
                outfile.write('\n')  # Add a newline character to separate each tweet

        print(f"Processed data has been written to '{output_json_file}'")
        return True

    except IOError as e:
        raise IOError(f"Error reading/writing file: {e}")