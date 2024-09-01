//#region @notForNpm

//#region @browser
    import { NgModule } from '@angular/core';
    import { Component, OnInit } from '@angular/core';


    @Component({
      selector: 'app-firedev-typeorm',
      template: 'hello from firedev-typeorm'
    })
    export class TaonTypeormComponent implements OnInit {
      constructor() { }

      ngOnInit() { }
    }

    @NgModule({
      imports: [],
      exports: [TaonTypeormComponent],
      declarations: [TaonTypeormComponent],
      providers: [],
    })
    export class TaonTypeormModule { }
    //#endregion

    //#region @backend
    async function start(port: number) {
      console.log('hello world from backend');
    }

    export default start;

//#endregion

//#endregion
