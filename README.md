# DataConveyer_AggregateTokens

DataConveyer_AggregateTokens is a console application to demonstrate how Data Conveyer can be
used to accumulate data extracted from a sequence of input files.

There are 10 sample XML files located in ...Data folder. Data Conveyer will process all these
files and identify tokens contained in them (in this example, tokens are just XML nodes named *Token*). The
 contents of these tokens will be accumulated and saved in a CSV file. 

## Installation

* Fork this repository and clone it onto your local machine, or

* Download this repository onto your local machine.

## Usage

1. Open DataConveyer_AggregateTokens application in Visual Studio.

2. Build and run the application, e.g. hit F5

    - a console window with directions will show up.

3. Hit any key into the console window to start the process

    - the files in the ...Data folder will get processed as reported in the console window.

4. Exit the application, hit Enter key into the console window.

5. Review the contents of the TokenAggregates.csv file that Data Conveyer placed in the ...Data folder.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

## Copyright

```
Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.
```
