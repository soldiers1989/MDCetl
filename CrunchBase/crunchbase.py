from Shared.common import Common as CM


class Crunchbase:
    def __init__(self):
        self.user_key = CM.get_config('config.ini', 'crunch_base', 'user_key')
        
        self.url_org = CM.get_config('config.ini', 'crunch_base', 'url_org') + '&user_key=' + self.user_key
        self.url_person = CM.get_config('config.ini', 'crunch_base', 'url_person')+ '?user_key=' + self.user_key
        self.url_prd = CM.get_config('config.ini', 'crunch_base', 'url_prd')
        self.url_cat = CM.get_config('config.ini', 'crunch_base', 'url_cat')
        self.url_loc = CM.get_config('config.ini', 'crunch_base', 'url_loc')
        
        self.organizations = None
        self.people = None
        self.products = None
        self.categories = None
        self.locations = None
        
    def get_organizations(self):
        self.organizations = CM.get_crunch_data(self.url_org)
    
    def get_people(self):
        self.people = CM.get_crunch_data(self.url_person)
        
        
if __name__ == '__main__':
    crb = Crunchbase()
    #crb.get_organizations()
    crb.get_people()
    print(crb.people.text)