# MDCetl

# `BAP Quarterly`
##This is a _quarterly_ process for the data collected from each of the **17** RICs.
        
        1. Communitech
        2. Haltech
        3. IION
        4. Innovate Niagara
        5. Innovation Factory
        6. Innovation Guelph
        7. Invest Ottawa
        8. Launch Lab
        9. MaRS
        10. NOIC
        11. NORCAT
        12. RIC Center
        13. Spark Center
        14. SSMIC
        15. Tech Alliance
        16. Venture Lab
        17. WeTech
        
##BAP Quarterly data processing steps

I. Read RIC spreadsheet file

        The spreadsheet file from the RICs is stored in a shared BOX file. The sysetem reads it automatically loads it 
        to the memory for QA.
II.  QA the spreadsheet
        
        The QA for each RICs spreadsheet is done using the **DataCheck** library for:
            1. Data Format
            2. Data Type
            3. Ranges (Upper and Lower bound)
            4. Correctness (Eg. Postal Code)
            5. etc...
        The result of the QA leads to two processes.
        If the QA passes for all the spreadsheet sheets, we proceed to the next step. If not, we either contact the RICs 
        to correct the data or we make a judgement call to correct the possible values or formatting as necessary.
III. Combine the spreadsheet in to three common group

        Once the QA is completed and data is complete to our staisfaction, we combine all the spreadsheet for the RICs
        (i.e 17 fo them), we combine the data in to one spreadsheet file per (three worksheets) and save it in the BOX
        file for the next round QA.
        
IV. Do another round of QA

        The details of this QA will be laid out in the days to come.
V. Push the data to raw data repository in the database

        The cleaned data will then pushed to the database as a raw data from the RICs.It will be pushed to :
            1. Config.CompanyProgram
            2. Config.CompanyProgramAgg
            2. Config.CompanyDataRaw
        The schema for this table should be changed to [Raw] or [DataSource].
VI. Create batch and update the raw data table with the new batch id

        A batch is created in the database per each RICs worksheet in the three catagories we collect data on. Each batch
        has the date imported, data source, system source, file name, file path(BOX file path) and other relevant info 
        about the source file submitted by each RIC. This helps us trace back the source of each data in our datawarehouse.
VII. Run company matching
        
        The company matching algorithm takes a single company form the new data source, checks if it exists in 
        MDC database (DimCompany and DimCompanySource) and performs insertion or update of the company.
        Here are the three cases happening at this stage:
            1. The new company doesn't exists in our database
                - Insert in to DimCompany table and get the new COMPANYID and insert the new record to DimCompanySOurce
            2. The new company exists only in DimCompany
                - Get the COMPANYID and insert to DimCompanySource
            3. THe new company exists only in DimCompanySource
                - Insert the new company to DimCompany and update the COMPANYID of DimCompanySource
            4. The new company exists in our database(both in DimCompany and DimCompanySource)
                - Do nothing
VIII. Insert company to the DimCompany (DimEntity) table and DimCompanySource

        Insert the new company to the aforementioned tables

IX. Push data to FactRICCompanyData table

        The data from the 'Company Data' sheet of the BAP Quarterly template will be later populated to FactRICCompanyData 
        once it is moved to the 'Raw' table and all the compaies 'CompanyID' and 'BatchID' has been updated. There are some 
        minor changes to the data structure when it pushed to this fact table.
 
X. Push data to FactRICAggregation table

        The program data and program youth data of the BQ template holds a bunch of aggregate values about the RICs activities 
        in regards to the companies they are working with. These aggregate values for the specific quarters will be 
        transposed and pushed to this table.

XI. Roll up date and insert it to FactRICAggregationRolledUp table
        
        

XII. Generate Report

XIII. QA Report
