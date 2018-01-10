import requests
from time import sleep
import pandas as pd
import codecs
import numpy as np
import datetime
import sys


class sg_contact_lists:

    @classmethod
    def sg_get_contactlists_json(self, api_token):
        '''Gets Survey Gizmo ContactList object (list of ContactLists).
        Returns as json dict.
        None -> dict'''

        base = "https://restapica.surveygizmo.com/v5/contactlist"
        url = base + "/?" + api_token
        for i in range(0, 10):
            try:
                output = requests.get(url)
                if output.ok:
                    output = output.json()
                    print("Success. ContactList json retrieved.")
                    return output
            except:
                print("Probable SSLError. Trying again in 3 seconds...")
                sleep(3)
                pass

    @classmethod
    def sg_contactlists_df(self, api_token):
        '''Takes json formatted list of contact lists
        and returns a dataframe.
        dict -> dataframe
        '''

        json = self.sg_get_contactlists_json(api_token)
        headers = ["id", "list_name", "date_created", "date_modified"]
        c_lists = []

        for i in json["data"]:
            clid = i["id"]
            list_name = i["list_name"]
            date_created = i["date_created"]
            date_mod = i["date_modified"]
            c_lists.append([clid, list_name, date_created, date_mod])

        df = pd.DataFrame(c_lists, columns=headers)
        return df

    @classmethod
    def sg_single_contactlist_json(self, list_id, api_token):
        '''Takes contactlist id and api token and returns
        json-formatted dict.
        int, str -> dict
        '''

        list_id = str(list_id)

        base = "https://restapica.surveygizmo.com/v5/contactlist"
        url = base + "/" + list_id + "/contactlistcontact" + "/?" + "resultsperpage=500" + "&" + api_token
        for i in range(0, 10):
            try:
                output = requests.get(url)
                if output.ok:
                    output = output.json()
                    print("Success. ContactList json retrieved.")
                    return output
            except:
                print("Probable SSLError. Trying again in 3 seconds...")
                sleep(3)
                pass

    @classmethod
    def sg_single_contactlist_df(self, list_id, api_token):
        '''Takes json-formatted dict and returns dataframe
        with contact info for each contact on list.
        dict -> dataframe
        '''

        json = self.sg_single_contactlist_json(list_id, api_token)
        clid = int(list_id)
        headers = ["id", "mdc_contact_id", "contact_list_id", "email_address", "firstname", "lastname", "organization", "division",
                   "dept", "group", "role", "team", "homephone", "faxphone", "businessphone", "mailingadress",
                   "mailingadress2", "mailaddresscity", "mailaddressstate", "mailaddresspostal", "mailaddresscountry",
                   "title", "url"]
        contacts = []
        for i in json["data"]:
            cid = i["id"]
            try:
                mdc_cid = i["contact_id"]
            except KeyError:
                # print("no MDC contact id associated w this company")
                mdc_cid = None
            email = i["email_address"]
            fname = i["first_name"]
            lname = i["last_name"]
            org = i["organization"]
            div = i["division"]
            dept = i["department"]
            group = i["group"]
            role = i["role"]
            team = i["team"]
            hphone = i["home_phone"]
            fax = i["fax_phone"]
            bphone = i["business_phone"]
            mail1 = i["mailing_address"]
            mail2 = i["mailing_address2"]
            mcity = i["mailing_address_city"]
            mstate = i["mailing_address_state"]
            mpost = i["mailing_address_postal"]
            mcntry = i["mailing_address_country"]
            title = i["title"]
            url = i["url"]
            contacts.append(
                [cid, mdc_cid, clid, email, fname, lname, org, div, dept, group, role, team, hphone, fax, bphone, mail1, mail2,
                 mcity, mstate, mpost, mcntry, title, url])

        df = pd.DataFrame(contacts, columns=headers)
        return df

    @classmethod
    def sg_contactlist_attribute(self, api_token, list_name, attribute="iGroupID"):
        '''Takes name of existing SurveyGizmo contact list and returns
        its specified attribute as a str. Returns id attr by default.
        str, str -> str
        '''

        list_name = str(list_name)
        attribute = str(attribute)
        contact_lists = self.sg_get_contactlists_json(api_token)
        for lst in contact_lists["data"]:
            try:
                if lst["list_name"] == list_name:
                    print("Attribute:", attribute, "is", lst[attribute])
                    return lst[attribute]
            except TypeError:
                continue
            except KeyError:
                continue

    @classmethod
    def sg_post_contacts(self, name, contacts, api_token):
        '''Takes list name and df of contacts and POSTs each row to
        Survey Gizmo Contact List named [name].
        str, dataframe -> None
        '''

        one_contact = []
        sg_vars = {}
        i = 0
        list_id = self.sg_contactlist_attribute(api_token, name, "id")

        for contact in contacts.itertuples(index=False):
            # fname = codecs.decode(str(contact[1]), 'unicode_escape')
            fname = str(contact[2])
            sg_fname = "&sfirstname=" + str(fname)
            # lname = codecs.decode(str(contact[2]), 'unicode_escape')
            lname = str(contact[3])
            lname = lname.replace("\\", "")
            # lname2 = bytes(lname, encoding='iso-8859-1')
            # lname3 = lname2.decode('unicode-escape')
            sg_lname = "&slastname=" + lname
            # email = codecs.decode(str(contact[3]), 'unicode_escape')
            email = str(contact[1])
            sg_email = "&semailaddress=" + str(email)
            # org = codecs.decode(str(contact[5]), 'unicode_escape')
            contact_id = str(contact[0])
            mdc_cid = "&contact_id=" + str(contact_id)

            one_contact.append([sg_fname, sg_lname, sg_email, mdc_cid])
            post_url = self.sg_post_contact_URL(list_id, one_contact[0], api_token)
            one_contact = []
            attempt_count = 0

            for i in range(0, 10):
                try:
                    attempt_count += 1
                    output = requests.post(post_url)
                except:
                    if attempt_count >= 10:
                        print("All attempts failed")
                        return
                    print("Likely SSLError. Trying again in 3 seconds...")
                    sleep(3)
            if output.ok:
                print("Contact " + str(contact[2]) + " " + str(contact[3]) + " added successfully.")
                sleep(0.05)
                output = None

    @classmethod
    def sg_post_contact_URL(self, list_id, contact_info, api_token):
        '''Takes lst of single contact's info, returns SurveyGizmo PUT
        URL to add to ContactList with id list_id.
        lst, str -> str
        '''
        list_id = "/" + list_id
        base = "https://restapica.surveygizmo.com/v4/contactlist"
        method = "?_method=POST"
        contact_fields = ''.join(contact_info)
        post_url = base + list_id + method + contact_fields + "&" + api_token
        return post_url

    @classmethod
    def sg_check_contactlist_exists(self, name, api_token):
        '''Check if Survey Gizmo ContactList of name already exists. Returns bool.
        str -> bool
        '''

        exists = name == self.sg_contactlist_attribute(api_token, name, "list_name")

        if exists:
            print("ContactList with that name already exists on this account.")
        return exists

    @classmethod
    def sg_put_contact_list(self, name, api_token):
        '''Takes API token, str name and creates new contact list on Survey Gizmo
        with name str.
        str -> None
        '''

        exists = self.sg_check_contactlist_exists(name, api_token)
        #     try:
        if exists:
            print("No new list was created.")
            return
            #     except:

        for i in range(0, 10):
            try:
                base = "https://restapica.surveygizmo.com/v4/contactlist?_method=PUT&listname="
                URL = base + str(name) + "&" + api_token
                output = requests.put(URL)
                if output.ok:
                    print("Success! Contact List", name, "created.")
                    return
            except:
                print("SSLError. Trying again in 3 seconds...")
                sleep(3)
                pass

    @classmethod
    def xl_to_contacts_df(self, filepath):
        '''Take str filepath (must be Excel file) and writes first sheet
        into dataframe.
        str -> dataframe
        '''

        contacts = pd.ExcelFile(filepath)
        return contacts.parse(contacts.sheet_names[0])

    # Process for uploading a new contact list is as follows:
    # Step 1: contacts = xl_to_contacts_df("pathstring")
    # (creates df from xl file's first sheet)
        #
    # Step 2: Call sg_put_contact_list("list name string", api_token)
    # (creates a new contact list on account, ready to be populated)
        #
    # Step 3: Call sg_post_contacts("list name str from above", contacts, api_token
    # (uploads each contact from contact df to the list named. Requires specific contact sheet format)
