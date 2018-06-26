class Update:

    @staticmethod
    def update(record1, record2):
        """
        Update existing record in database where fields are null with new info in fields

        :param record1: dictionary of field values in database ex [Description:'' ,Phone:'' ,Address:'' ,Website:'' ]
        :param record2: list of new field values being used for updating
        :return:
        """
        for index, field in record1.items():
            if index == 'ID' or index == 'Name' or index == 'BatchID':
                continue
            else:
                if record2[index] is not None:
                    if index == 'AlternateName':
                        record1[index] = record1[index] + '|' + record2[index]
                    else:
                        record1[index] = record2[index]
        return record1

    @staticmethod
    def update_all(record1, record2):
        # Update  update all the fields of existing record in database where record2 has data
        for index, field in record1.items():
            record1[index] = record2[index]
        return record1
