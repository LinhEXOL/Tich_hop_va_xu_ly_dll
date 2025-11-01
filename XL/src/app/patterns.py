# coding=utf-8
framework_plattforms = ['Docker', 'OSP', 'Premiere', 'directAdmin', 'typography', 'Prometheus', 'visual weight', 'Kubernetes', 'JDBC', 'UnitTest',
            'Servlets', 'cPanel', 'MySQL', '.NET', 'Ruby on Rails', 'JSP', 'IdentityServer', 'VoIP', 'AdobeXD', 'CMake', 'Autocad',
            'Spring', 'Django', 'CRM', 'K8S', 'Nginx', 'firmware', 'Google Trend', 'psd', 'CSRF', 'Reactjs', 'Struts', 'WebSocket',
            'Webpack', 'Spine', 'Vue', 'METEOR', 'Rancher', 'VFX', 'node js', 'Angular', 'Flask', 'ASP.NET', 'Google Analystics', 'Zend',
            'Symfony', 'Express', 'Google Protobuf', 'J2EE', 'Ansible', 'WebForm', 'Videoscribe', 'CakePHP', 'Hibernate', 'Git', 'Oracle',
            'Plesk', 'Log4j', 'JSON', 'Visio', 'Grafana', 'SDLC', ' EELinux', ' Redis', 'Redux', 'WinForm', 'Figma', 'CodeIgniter',
            'Power BI', 'Bootstrap', 'WPF', 'Aerospike', 'bash shell', 'Laravel', 'SQL Server']

design_patterns=["MVC"," Singleton"," WPF", " MVVM","Session Facade", " DAO ", "OOA/OOD","Factory Pattern",
                'Microservice']

knowledges = ['game', 'Jira', 'lắp đặt', 'interaction design', 'đồ họa', 'DevOps', ' AI ', ' async', 'Quality Assurance',
               ' Security ', 'Google Drive', 'NFT', 'mạng máy tính', 'Wordpress', 'Machine Learning', 'Consult', 'White Box', 'sale',
               'kiểm thử', 'đánh giá chất lượng', 'networking', 'distributed system', 'UI/UX', 'Windows', 'Unit Test',
               'Jenkins', 'Chatbot', 'quản trị mạng', 'Solidity', 'tester', 'Corel Draw', 'Illustrator', 'Git', 'Android','phân tích dữ liệu',
               'Black Box', 'Office', 'chạy quảng cáo', 'Unix', 'IT Support', 'Data mining', 'data analys','Cài đặt','hệ điều hành', 'cấu trúc dữ liệu', 'Switch',
               ' TCP', 'qa', 'Animate', 'crypto', 'CI/CD', 'Defi', 'frontend', 'sửa chữa','Kinh tế', 'SVN', 'phần cứng', ' sync','BrSE', 'bảo mật',
               'Powerpoint', 'smart contract', 'Linux', 'SCM', 'backend', 'Marketing', 'XSS', 'Photoshop', 'HTTP', 'Word', 'router', 'IOS',
               'WebSocket', 'thuật toán', 'TestRail', 'CSDL', 'Sketch', 'blockchains', 'multithreading', 'hướng đối tượng', 'Front-end',
               'latex', 'Restful', 'Subversion', 'java web', 'Mobile', 'Excel', 'design pattern']

labeled_knowledges={'AI': 'AI', 'Machine Learning': 'AI', 'Data mining': 'AI', 'Chatbot': 'AI', 'data analys': 'AI',

                    'blockchains': 'blockchain_crypto', 'crypto': 'blockchain_crypto', 'NFT': 'blockchain_crypto',
                    'smart contract': 'blockchain_crypto', 'Solidity': 'blockchain_crypto', 'Defi': 'blockchain_crypto',
                    'XSS': 'blockchain_crypto',
                    'data analys': 'Data', 'Data mining':'Data', 'phân tích dữ liệu':'Data',
                    'lắp đặt': 'hardware', 'sửa chữa': 'hardware', 'phần cứng': 'hardware', 'router': 'hardware', 'cài đặt': 'hardware',
                    'Corel Draw': 'hardware', 'Switch': 'hardware',

                    'Word': 'office', 'Excel': 'office', 'Powerpoint': 'office', 'Office': 'office',

                    'Illustrator': 'photoshop', 'Photoshop': 'photoshop', 'Animate': 'photoshop',

                    'cấu trúc dữ liệu': 'programming_basic', 'thuật toán': 'programming_basic', 'OOP': 'programming_basic',
                    'hướng đối tượng': 'programming_basic', 'hệ điều hành': 'programming_basic', 'multithreading': 'programming_basic',

                    'Black Box': 'tester', 'tester': 'tester', 'White Box': 'tester', 'Unit Test': 'tester',
                    'TestRail': 'tester', 'kiểm thử': 'tester',

                    'bảo mật':'security', 'Security': 'security',

                    'SVN': 'version_control', 'SCM': 'version_control', 'Git': 'version_control',
                    'Front-end': 'Web','frontend': 'Web', 'backend':'Web', 'java web':'Web',
                    'mạng máy tính': 'Mạng', 'quản trị mạng':'Mạng'}

knowledge_groups={
    "blockchain_crypto":["blockchains","crypto","NFT","smart contract","Solidity", "Defi",'XSS' ]    ,
    "office":["Word", "Excel","Powerpoint","Office"],
    "AI":[" AI", "Machine Learning","Data mining", 'Chatbot',"data analys"],
    "tester":["Black Box", "tester", 'White Box', "Unit Test",'TestRail', "kiểm thử"],
    "programming_basic": ["cấu trúc dữ liệu","thuật toán","OOP","hướng đối tượng"],
    "version_control":["SVN", "SCM","Git"],
    "hard_ware":[ "lắp đặt",'cài đặt', "sửa chữa", "phần cứng","router",'Corel Draw',"Switch"],
    "photoshop": ["Illustrator","Photoshop","Animate"],

}

IT_languages = [' CHAIN ', ' ABAP ', 'Lingo', ' CPL', 'NPL', 'Xtend', ' Flex ', ' Io ', 'Erlang', 'Python', 'MSL', 'SAIL', ' Chef ', 'RPG', 'PHP',
             'XPath', 'Lava', ' Clojure', 'Mathematica', 'QPL', 'Oak', 'Objective-C', 'P#', 'css', ' Delphi', ' A+ ', 'XQuery',
             'CoffeeScript', ' SR ', 'ECMAScript', 'Nial', ' Red ', 'Mesa', 'LSL', 'T-SQL', 'E#', ' GAP ', 'Simula', 'Logo', ' Caml',
             'JavaScript', 'PeopleCode', ' UNITY ', ' Blue ', 'Span', 'Lucid', ' BeanShell', ' CSP', 'Scheme', 'Swift', 'TypeScript',
             ' Scala ', 'Scratch', 'Strand', 'XML', ' SAS', 'PortablE', 'JADE', ' C ', 'Processing', 'Pure', ' BASIC ', ' Bash', 'Pro*C',
             'ROOP', 'PL/SQL', 'Icon', ' Dart', ' Factor ', 'Java', 'LINC', ' Go ', ' TIE ', ' Cool', 'Kotlin', 'Rust', 'Opa', 'DYNAMO',
             ' Inform ', 'Mary', 'Ruby', 'YQL', 'Pike', ' rc ', ' html ', 'Oz', 'Groovy', 'PowerShell', ' CUDA', 'Hack', ' Self ',
             ' CFEngine', 'C#', 'SPS']
languages= {'Tiếng Anh':'Tiếng Anh',  'Tiếng Nhật':  'Tiếng Nhật', 'Japan': 'Tiếng Nhật', 'English':'Tiếng Anh', 'Tiếng Trung':'tiếng trung'  }
salary_patterns = ["lương(?:từ| )+ ((?:\d+|\.)+)", "((?:\d+|\.|-| )+(?:triệu| )+)đồng",
                   "(?:\d|\.|,)+.000.000", "(?:\d+| |-)+\d+ *(?:triệu|m)", "\$(?:\d+|,)", "(?:\d+|,)+ *(?:USD|\$)+",
                   "(?:\d|\.|,)+,000,000"]


provinces=['An Giang','Bà Rịa-Vũng Tàu','Bắc Giang','Bắc Kạn','Bạc Liêu','Bắc Ninh','Bến Tre','Bình Định','Bình Dương','Bình Phước','Bình Thuận','Cà Mau','Cần Thơ','Cao Bằng','Đà Nẵng','Đắk Lắk','Đắk Nông','Điện Biên','Đồng Nai','Đồng Tháp','Gia Lai','Hà Giang','Hà Nam','Hà Nội','Hà Tĩnh','Hải Dương','Hải Phòng','Hậu Giang','Hồ Chí Minh','Hòa Bình','Hưng Yên','Khánh Hòa','Kiên Giang','Kon Tum','Lai Châu','Lâm Đồng','Lạng Sơn','Lào Cai','Long An','Nam Định','Nghệ An','Ninh Bình','Ninh Thuận','Phú Thọ','Phú Yên','Quảng Bình','Quảng Nam','Quảng Ngãi','Quảng Ninh','Quảng Trị','Sóc Trăng','Sơn La','Tây Ninh','Thái Bình','Thái Nguyên','Thanh Hóa','Thừa Thiên - Huế','Tiền Giang','Trà Vinh','Tuyên Quang','Vĩnh Long','Vĩnh Phúc','Yên Bái']

educations= ['Trung cấp', 'Cao đẳng', 'Cử nhân', 'Kỹ sư', 'Trung học phổ thông', 'Thạc sĩ', 'Tiến sĩ', 'Chứng chỉ', ]