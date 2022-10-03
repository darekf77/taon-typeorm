//#region @notForNpm

//#region @browser
    import { NgModule } from '@angular/core';
    import { Component, OnInit } from '@angular/core';


    @Component({
      selector: 'app-firedev-typeorm',
      template: 'hello from firedev-typeorm'
    })
    export class FiredevTypeormComponent implements OnInit {
      constructor() { }

      ngOnInit() { }
    }

    @NgModule({
      imports: [],
      exports: [FiredevTypeormComponent],
      declarations: [FiredevTypeormComponent],
      providers: [],
    })
    export class FiredevTypeormModule { }
    //#endregion

    //#region @backend
    async function start(port: number) {
      console.log('hello world from backend');
    }

    export default start;

//#endregion

//#endregion