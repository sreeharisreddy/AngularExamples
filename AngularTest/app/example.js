var app = angular.module('myapp',[])
app.controller('HelloController',function($scope){
$scope.sayHI="hello";
});

(function() {
  'use strict';

  // Creating the module and factory we referenced in the beforeEach blocks in our test file
  angular.module('api.users', [])
  .factory('Users', function() {
    var Users = {};
 var userList = [
      {
        id: '1',
        name: 'Sreehari',
        role: 'Designer',
        location: 'Singapore',
        salary:'5000'
      },
      {
        id: '2',
        name: 'Gayathri',
        role: 'Developer',
        location: 'Singapore',
        salary:'4000'
      },
      {
        id: '3',
        name: 'Nikki',
        role: 'Developer',
        location: 'Atmakur',
        salary:'3000'
      },
      {
        id: '4',
        name: 'Joshi',
        role: 'Designer',
        location: 'LA',
        salary:'2000'
      }
    ];
 
	Users.all = function() {
		return userList;
    };
	
	Users.findById = function(id) {
		return userList.find(function(user){
		  return user.id === id;
		});
    };
    return Users;
  });
})();