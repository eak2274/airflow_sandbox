import requests

def print_random_quote():
    response = requests.get('https://zenquotes.io/api/random')
    quote = response.json()
    print(quote)
    print(quote[0]['q'])
    # print('Quote of the day: "{}"'.format(quote))

print_random_quote()