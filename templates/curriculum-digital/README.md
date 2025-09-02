# Curriculum Digital

## Overview
Curriculum Digital is a web application designed to manage personnel data efficiently. It provides a comprehensive API for handling employee information, including personal details, educational background, work experience, and address information.

## Features
- **API Endpoints**: The application exposes various API endpoints for managing employee data, including:
  - Authentication
  - Employee management
  - Assistance requests
  - Dashboard statistics

- **Detailed Address Input**: The application includes a detailed address input section that allows users to specify:
  - Localidad (locality)
  - Inmueble (property type)
  - Estado (state)
  - Ciudad (city)
  - Municipio (municipality)
  - Parroquia (parish)
  - Zona Postal (postal zone)

  This section features dropdowns for selecting "localidad" and "inmueble," with dependent fields for the other address components. The application ensures that all necessary tables for address management are created and checked for existence.

## Installation
To set up the project, follow these steps:

1. Clone the repository:
   ```
   git clone <repository-url>
   cd curriculum-digital
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the application:
   ```
   python app.py
   ```

## Usage
- Access the API documentation to explore available endpoints and their functionalities.
- Use the detailed address input section to enter comprehensive address information for personnel.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.